package org.meteor;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.ServerConfig;
import org.meteor.coordinatior.Coordinator;
import org.meteor.coordinatior.DefaultCoordinator;
import org.meteor.internal.MeteorServer;
import org.meteor.listener.MetricsListener;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.thread.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

public class Meteor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Meteor.class);
    public static void main(String[] args) throws Exception {
        start(createServer(args));
    }

    private static void start(MeteorServer server) {
        try {
            server.start();
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start meteor broker server failed", e);
            }
            System.exit(-1);
        }
    }

    private static MeteorServer createServer(String... args) throws Exception {
        Properties properties = loadConfigurationProperties(args);
        ServerConfig configuration = new ServerConfig(properties);

        Coordinator coordinator = new DefaultCoordinator(configuration);
        MetricsListener metricsListener = new MetricsListener(properties, configuration.getCommonConfig(), configuration.getMetricsConfig(), coordinator);
        coordinator.addMetricsListener(metricsListener);
        DefaultSocketServer socketServer = new DefaultSocketServer(configuration, coordinator);
        return initializeServer(metricsListener, socketServer, coordinator);
    }

    private static Properties loadConfigurationProperties(String... args) throws Exception {
        Options options = constructCommandlineOptions();
        DefaultParser parser = new DefaultParser();
        Properties properties = new Properties();

        try {
            CommandLine commandLine = parser.parse(options, args);
            String file;
            if (commandLine.hasOption('c')) {
                file = commandLine.getOptionValue('c');
                if (!StringUtil.isNullOrEmpty(file)) {
                    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                        properties.load(in);
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof MissingOptionException) {
                throw new MissingOptionException("Please set the broker.properties path, use [-c]");
            }
            throw e;
        }
        return properties;
    }

    private static MeteorServer initializeServer(MetricsListener listener, DefaultSocketServer socketServer, Coordinator coordinator) {
        MeteorServer server = new MeteorServer(socketServer, coordinator);
        server.addListener(listener);
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<?>) () -> {
            server.shutdown();
            return null;
        }).newThread());
        return server;
    }

    private static Options constructCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "configFile", true, "Meteor broker server config file");
        option.setRequired(true);
        options.addOption(option);
        return options;
    }
}
