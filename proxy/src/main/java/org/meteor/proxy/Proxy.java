package org.meteor.proxy;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.meteor.core.CoreConfig;
import org.meteor.core.MeteorServer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.util.TypeTransformUtils;
import org.meteor.listener.MetricsListener;
import org.meteor.management.Manager;
import org.meteor.proxy.beans.ProxyBean;
import org.meteor.util.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.Callable;

public class Proxy {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Proxy.class);
    public static void main(String[] args) {
        try {
            start(createServer(args));
        } catch (Exception e) {
            logger.error("Start meteor proxy server failed", e);
            System.exit(-1);
        }
    }

    private static void start(MeteorServer server) throws Exception {
        server.start();
    }

    private static MeteorServer createServer(String... args) throws Exception {
        Options options = buildCommandlineOptions();
        CommandLine commandLine = parseCmdLine(args, options, new DefaultParser());
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

        ProxyBean.init(properties);
        CoreConfig config = ProxyBean.getBean(CoreConfig.class);
        printConfig(config);

        Manager manager = ProxyBean.getBean(Manager.class);
        MetricsListener metricsListener = ProxyBean.getBean(MetricsListener.class);
        manager.addMetricsListener(metricsListener);

        MeteorServer server = ProxyBean.getBean(MeteorServer.class);
        server.addListener(metricsListener);

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(logger, (Callable<?>) () -> {
            server.shutdown();
            return null;
        }).newThread());

        return server;
    }

    private static Options buildCommandlineOptions() {
        Options options = new Options();
        Option option = new Option("c", "configFile", true, "Server config file");
        option.setRequired(true);
        options.addOption(option);

        return options;
    }

    private static CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) throws ParseException {
        return parser.parse(options, args);
    }

    private static void printConfig(CoreConfig config) {
        Method[] declaredMethods = CoreConfig.class.getDeclaredMethods();
        StringBuilder sb = new StringBuilder("Print the config options: \n");
        String configName;
        for (Method method : declaredMethods) {
            String name = method.getName();
            if (name.startsWith("get")) {
                configName = name.substring(3);
                checkReturnType(method, config, sb, configName);
            }

            if (name.startsWith("is")) {
                configName = name.substring(2);
                checkReturnType(method, config, sb, configName);
            }
        }
        logger.info(sb.toString());
    }

    private static void checkReturnType(Method method, CoreConfig config, StringBuilder sb, String name) {
        String simpleName = method.getReturnType().getSimpleName();
        Object invoke;
        try {
            switch (simpleName) {
                case "int", "Integer" -> invoke = TypeTransformUtils.object2Int(method.invoke(config));
                case "long", "Long" -> invoke = TypeTransformUtils.object2Long(method.invoke(config));
                case "double", "Double" -> invoke = TypeTransformUtils.object2Double(method.invoke(config));
                case "float", "Float" -> invoke = TypeTransformUtils.object2Float(method.invoke(config));
                case "boolean", "Boolean" -> invoke = TypeTransformUtils.object2Boolean(method.invoke(config));
                case "String" -> invoke = TypeTransformUtils.object2String(method.invoke(config));
                default -> throw new IllegalArgumentException(String.format("Illegal parameter<%s>", name));
            }
            sb.append(String.format("\t %s = %s", name, invoke)).append("\n");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }
}
