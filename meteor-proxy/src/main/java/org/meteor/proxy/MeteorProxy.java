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

    private static Options constructCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "configFile", true, "Meteor proxy server config file");
        option.setRequired(true);
        options.addOption(option);

        return options;
    }
}
