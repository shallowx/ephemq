package org.meteor.proxy;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.meteor.configuration.ServerConfiguration;
import org.meteor.internal.MeteorServer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.listener.MetricsListener;
import org.meteor.coordinatio.Coordinator;
import org.meteor.proxy.coordinatio.ZookeeperProxyCoordinator;
import org.meteor.proxy.net.MeteorProxyServer;
import org.meteor.proxy.net.ProxySocketServer;
import org.meteor.util.ShutdownHookThread;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
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
            logger.error("Start meteor proxy server failed", e);
            System.exit(-1);
        }
    }

    private static MeteorServer createServer(String... args) throws Exception {
        Options options = constructCommandlineOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
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

        ServerConfiguration configuration = new ServerConfiguration(properties);

        Coordinator manager = new ZookeeperProxyCoordinator(configuration);
        MetricsListener metricsListener = new ProxyMetricsListener(properties, configuration.getCommonConfiguration(), configuration.getMetricsConfiguration(), manager);
        manager.addMetricsListener(metricsListener);

        ProxySocketServer socketServer = new ProxySocketServer(configuration, manager);
        MeteorServer server = new MeteorProxyServer(socketServer, manager);
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
