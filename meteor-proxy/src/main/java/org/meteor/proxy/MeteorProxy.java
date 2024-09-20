package org.meteor.proxy;

import io.netty.util.internal.StringUtil;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
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
import org.meteor.internal.MeteorServer;
import org.meteor.listener.MetricsListener;
import org.meteor.proxy.core.ProxyMetricsListener;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.proxy.remoting.MeteorProxyServer;
import org.meteor.proxy.remoting.ProxySocketServer;
import org.meteor.proxy.support.ProxyDefaultManager;
import org.meteor.support.Manager;
import org.meteor.thread.ShutdownHookThread;

/**
 * The MeteorProxy class is responsible for starting and configuring the
 * Meteor proxy server. It parses command line arguments, loads properties
 * from a specified configuration file, and initializes the server with the
 * appropriate settings.
 */
public class MeteorProxy {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);

    public static void main(String[] args) throws Exception {
        start(createServer(args));
    }

    /**
     * Starts the given MeteorServer instance. If an exception occurs during server startup,
     * an error message is logged, and the application exits with a status code of -1.
     *
     * @param server the MeteorServer instance to be started
     */
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

    /**
     * Creates an instance of MeteorProxyServer by parsing command line arguments,
     * loading properties configuration, and configuring the server accordingly.
     *
     * @param args Command line arguments used for initializing the server configuration.
     * @return An initialized instance of MeteorProxyServer based on the provided properties and configuration.
     * @throws Exception if any errors occur during command line parsing, properties loading, or server configuration.
     */
    private static MeteorProxyServer createServer(String... args) throws Exception {
        CommandLine commandLine = parseCommandLine(args);
        Properties properties = loadPropertiesFromFile(commandLine);
        ProxyServerConfig configuration = new ProxyServerConfig(properties);
        return configureMeteorProxyServer(configuration, properties);
    }

    /**
     * Parses command line arguments to extract and validate options.
     *
     * @param args command line arguments to be parsed
     * @return a CommandLine object containing the parsed options
     * @throws Exception if an error occurs during parsing, such as a missing required option
     */
    private static CommandLine parseCommandLine(String... args) throws Exception {
        Options options = constructCommandlineOptions();
        DefaultParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (Exception e) {
            if (e instanceof MissingOptionException) {
                throw new IllegalStateException("Please set the proxy.properties path, use [-c]");
            }
            throw e;
        }
    }

    /**
     * Loads properties from a file specified in the command line options.
     *
     * @param commandLine The CommandLine object containing command line options and arguments.
     * @return A Properties object loaded with the properties from the specified file.
     * @throws IOException If an I/O error occurs when reading the properties file.
     */
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

    /**
     * Configures and initializes a MeteorProxyServer instance with the specified configuration
     * and properties. This method sets up server components, adds metrics listeners, and registers
     * a shutdown hook for proper server shutdown.
     *
     * @param configuration the ProxyServerConfig object containing the server configuration settings
     * @param properties    the Properties object containing configuration properties required for server setup
     * @return a configured and initialized MeteorProxyServer instance
     */
    private static MeteorProxyServer configureMeteorProxyServer(ProxyServerConfig configuration, Properties properties) {
        Manager manager = new ProxyDefaultManager(configuration);
        MetricsListener metricsListener =
                new ProxyMetricsListener(properties, configuration.getCommonConfig(), configuration.getMetricsConfig(),
                        manager);
        manager.addMetricsListener(metricsListener);

        ProxySocketServer socketServer = new ProxySocketServer(configuration, manager);
        MeteorProxyServer server = new MeteorProxyServer(socketServer, manager);
        server.addListener(metricsListener);

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<?>) () -> {
            server.shutdown();
            return null;
        }).newThread());
        return server;
    }

    /**
     * Constructs and initializes the command line options required for the Meteor proxy server.
     *
     * @return Options object containing the necessary command line options configured.
     */
    private static Options constructCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "configFile", true, "Meteor proxy server config file");
        option.setRequired(true);
        options.addOption(option);

        return options;
    }
}
