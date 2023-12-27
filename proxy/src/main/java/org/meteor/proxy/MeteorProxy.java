package org.meteor.proxy;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.meteor.internal.MeteorServer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.listener.MetricsListener;
import org.meteor.coordinatior.Coordinator;
import org.meteor.proxy.coordinatior.ProxyDefaultCoordinator;
import org.meteor.proxy.internal.ProxyMetricsListener;
import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.proxy.net.MeteorProxyServer;
import org.meteor.proxy.net.ProxySocketServer;
import org.meteor.thread.ShutdownHookThread;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

public class MeteorProxy {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    public static void main(String[] args) throws Exception {
        start(createServer(args));
    }

    private static void start(MeteorServer server) {
        try {
            server.start();
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start meteor proxy server failed", e);
            }
            System.exit(-1);
        }
    }

    private static MeteorProxyServer createServer(String... args) throws Exception {
        CommandLine commandLine = parseCommandLine(args);
        Properties properties = loadPropertiesFromFile(commandLine);
        ProxyServerConfig configuration = new ProxyServerConfig(properties);
        return configureMeteorProxyServer(configuration, properties);
    }

    private static CommandLine parseCommandLine(String... args) throws ParseException {
        Options options = constructCommandlineOptions();
        DefaultParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Properties loadPropertiesFromFile(CommandLine commandLine) throws IOException {
        Properties properties = new Properties();
        String file;
        if (commandLine.hasOption('c')) {
            file = commandLine.getOptionValue('c');
            if (!StringUtil.isNullOrEmpty(file)) {
                try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                    properties.load(in);
                }
            }
        }
        return properties;
    }

    private static MeteorProxyServer configureMeteorProxyServer(ProxyServerConfig configuration, Properties properties) {
        Coordinator coordinator = new ProxyDefaultCoordinator(configuration);
        MetricsListener metricsListener = new ProxyMetricsListener(properties, configuration.getCommonConfig(), configuration.getMetricsConfig(), coordinator);
        coordinator.addMetricsListener(metricsListener);

        ProxySocketServer socketServer = new ProxySocketServer(configuration, coordinator);
        MeteorProxyServer server = new MeteorProxyServer(socketServer, coordinator);
        server.addListener(metricsListener);

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<?>) () -> {
            server.shutdown();
            return null;
        }).newThread());
        return server;
    }

    private static Options constructCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "configFile", true, "Server config file");
        option.setRequired(true);
        options.addOption(option);

        return options;
    }
}
