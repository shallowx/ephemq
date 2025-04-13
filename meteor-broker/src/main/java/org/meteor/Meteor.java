package org.meteor;

import io.netty.util.internal.StringUtil;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.ServerConfig;
import org.meteor.internal.MeteorServer;
import org.meteor.listener.MetricsListener;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.DefaultMeteorManager;
import org.meteor.support.Manager;
import org.meteor.support.ShutdownHookThread;

public class Meteor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Meteor.class);

    public static void main(String[] args) throws Exception {
        start(createServer(args));
    }

    /**
     * Starts the provided MeteorServer instance and handles any exceptions that occur during startup.
     * If an exception is caught and logging is enabled, logs an error message and terminates the JVM.
     *
     * @param server the MeteorServer instance to be started.
     */
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

    /**
     * Creates a MeteorServer instance by loading configuration properties, setting up the server configuration,
     * initializing the manager and metrics listener, and preparing the default socket server.
     *
     * @param args the command line arguments used to load configuration properties
     * @return a newly initialized MeteorServer
     * @throws Exception if an error occurs during the server creation process, including configuration loading,
     *                   metric listener setup, or server initialization
     */
    private static MeteorServer createServer(String... args) throws Exception {
        Properties properties = loadConfigurationProperties(args);
        ServerConfig configuration = new ServerConfig(properties);

        Manager manager = new DefaultMeteorManager(configuration);
        MetricsListener metricsListener =
                new MetricsListener(properties, configuration.getCommonConfig(), configuration.getMetricsConfig(),
                        manager);
        manager.addMetricsListener(metricsListener);
        DefaultSocketServer socketServer = new DefaultSocketServer(configuration, manager);
        return initializeServer(metricsListener, socketServer, manager);
    }

    /**
     * Loads configuration properties from a specified properties file provided as a command line argument.
     *
     * @param args the command line arguments used to specify the configuration file path
     * @return a Properties object containing the loaded configuration properties
     * @throws Exception if there are issues with file access, parsing command line arguments, or loading properties
     */
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

    /**
     * Initializes and configures a MeteorServer instance.
     *
     * @param listener the MetricsListener to be added to the server for metric monitoring.
     * @param socketServer the DefaultSocketServer to handle socket connections.
     * @param manager the Manager responsible for managing server operations and services.
     * @return an initialized MeteorServer instance ready for operation.
     */
    private static MeteorServer initializeServer(MetricsListener listener, DefaultSocketServer socketServer,
                                                 Manager manager) {
        MeteorServer server = new MeteorServer(socketServer, manager);
        server.addListener(listener);
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<?>) () -> {
            server.shutdown();
            return null;
        }).newThread());
        return server;
    }

    /**
     * Constructs and configures command line options for the Meteor broker server.
     * This method adds an option for specifying the config file, which is required for the server.
     *
     * @return an Options object containing the configured command line options
     */
    private static Options constructCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "config-file", true, "Meteor broker server config file");
        option.setRequired(true);
        options.addOption(option);
        return options;
    }
}
