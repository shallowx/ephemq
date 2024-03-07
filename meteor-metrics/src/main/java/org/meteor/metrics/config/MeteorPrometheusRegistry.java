package org.meteor.metrics.config;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MeteorPrometheusRegistry implements MetricsRegistrySetUp {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorPrometheusRegistry.class);
    private HttpServer server;
    private MeterRegistry registry;

    @Override
    public void setUp(Properties props) {
        MetricsConfig config = MetricsConfig.fromProps(props);
        if (config.isMetricsEnabled()) {
            this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Metrics.addRegistry(this.registry);

            exportHttpServer(config);
        }
    }

    private void exportHttpServer(MetricsConfig config) {
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(config.getMetricsAddress(), config.getMetricsPort());
            this.server = HttpServer.create(socketAddress, 0);

            String url = config.getMetricsScrapeUrl();
            this.server.createContext(url, exchange -> {
                String scrape = ((PrometheusMeterRegistry) this.registry).scrape();
                exchange.sendResponseHeaders(HttpResponseStatus.OK.code(),
                        scrape.getBytes(StandardCharsets.UTF_8).length);

                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(scrape.getBytes());
                }
            });
            new Thread(this.server::start).start();
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

    @Override
    public void shutdown() {
        if (server != null) {
            this.server.stop(0);
        }
    }
}
