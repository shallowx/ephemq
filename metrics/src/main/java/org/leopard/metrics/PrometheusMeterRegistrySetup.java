package org.leopard.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PrometheusMeterRegistrySetup implements MeterRegistrySetup {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PrometheusMeterRegistrySetup.class);

    private HttpServer server;
    private MeterRegistry registry;

    @Override
    public void setUp(Properties props) {
        MeterConfig config = MeterConfig.exchange(props);
        if (config.getMetricsEnabled()) {
            this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Metrics.addRegistry(registry);

            setHttpServer(config);
        }
    }

    private void setHttpServer(MeterConfig config) {
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(config.getMetricsAddress(), config.getMetricsPort());
            this.server = HttpServer.create(socketAddress, 0);

            String url = config.getMetricsScrapeUrl();

            this.server.createContext(url, exchange -> {
                String scrape = ((PrometheusMeterRegistry) registry).scrape();
                exchange.sendResponseHeaders(200, scrape.getBytes(StandardCharsets.UTF_8).length);

                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(scrape.getBytes());
                }
            });
            new Thread(this.server::start);

            if (logger.isInfoEnabled()) {
                logger.info("Metrics http server is listening at {}, and scrape url={}", socketAddress, url);
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Start metrics http server failed");
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
