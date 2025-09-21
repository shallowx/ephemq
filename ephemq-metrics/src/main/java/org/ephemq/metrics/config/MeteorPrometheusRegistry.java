package org.ephemq.metrics.config;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * The `MeteorPrometheusRegistry` class implements the `MetricsRegistrySetUp` interface to
 * set up and manage a Prometheus metrics registry. It initializes the Prometheus metrics
 * environment and sets up an HTTP server for scraping metrics.
 * <p>
 * This class is responsible for:
 * - Configuring the metrics registry from provided properties.
 * - Starting an HTTP server to expose the Prometheus metrics endpoint.
 * - Shutting down the HTTP server during cleanup.
 */
public class MeteorPrometheusRegistry implements MetricsRegistrySetUp {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorPrometheusRegistry.class);
    /**
     * The `HttpServer` instance used to expose the Prometheus metrics endpoint.
     * It creates an HTTP server to handle incoming HTTP requests for the
     * Prometheus metrics scraping.
     * <p>
     * Responsibilities include:
     * - Creating and configuring an HTTP server bound to a specified address and port.
     * - Defining a context for handling HTTP requests that scrape Prometheus metrics.
     * - Starting the HTTP server in a separate thread to serve the metrics endpoint.
     * - Shutting down the HTTP server during the cleanup process.
     */
    private HttpServer server;
    /**
     * Holds the MeterRegistry instance used to record and manage application's metrics.
     * The registry is configured based on the given properties and is integral for
     * Prometheus metrics collection and scraping.
     */
    private MeterRegistry registry;

    /**
     * Sets up the Prometheus metrics registry based on the provided properties.
     * If metrics are enabled, it initializes the PrometheusMeterRegistry and
     * exports the metrics via an HTTP server.
     *
     * @param props Properties object containing configuration settings for metrics.
     */
    @Override
    public void setUp(Properties props) {
        MetricsConfig config = MetricsConfig.fromProps(props);
        if (config.isMetricsEnabled()) {
            this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Metrics.addRegistry(this.registry);
            exportHttpServer(config);
        }
    }

    /**
     * Exports the Prometheus metrics via an HTTP server using the provided configuration.
     *
     * @param config The configuration object containing settings for the metrics address, port, and scrape URL.
     */
    private void exportHttpServer(MetricsConfig config) {
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(config.getMetricsAddress(), config.getMetricsPort());
            this.server = HttpServer.create(socketAddress, 0);
            this.server.setExecutor(command -> Thread.ofVirtual().start(command));
            String url = config.getMetricsScrapeUrl();
            if (!url.startsWith("/")) {
                url = "/" + url;
            }

            this.server.createContext(url, exchange -> {
                String scrape = ((PrometheusMeterRegistry) this.registry).scrape();
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(HttpResponseStatus.OK.code(), scrape.getBytes(StandardCharsets.UTF_8).length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(scrape.getBytes());
                }
            });

            Thread startThread = new Thread(this.server::start, "prometheus_httpServer_thread");
            startThread.setDaemon(true);
            startThread.start();
            if (logger.isInfoEnabled()) {
                logger.info("Prometheus http server is listening at socket address[{}], and scrape url[{}]", socketAddress, url);
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Start prometheus http server failed", t);
            }
            throw new RuntimeException(t);
        }
    }

    /**
     * Shuts down the HTTP server used for exposing the Prometheus metrics endpoint.
     * <p>
     * This method stops the HTTP server if it is currently running, releasing any resources
     * and ports that the server was using.
     */
    @Override
    public void shutdown() {
        if (server != null) {
            this.server.stop(0);
        }
    }
}
