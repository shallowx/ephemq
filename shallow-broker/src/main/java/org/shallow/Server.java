package org.shallow;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.shallow.internal.BrokerConfig;
import org.shallow.internal.BrokerServer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

public class Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Server.class);

    public static void main( String[] args ) {
        try {
            start(createShallowServer(args));
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start server failed", e);
            }
            System.exit(-1);
        }
    }

    private static BrokerServer start(BrokerServer server) throws Exception {
        server.start();
        return server;
    }

    private static BrokerServer createShallowServer(String[] args) throws Exception {
        Options options = buildCommandOptions();
        CommandLine cmdLine = parseCmdLine(args, options, new DefaultParser());

        Properties properties = new Properties();
        String file;
        if (cmdLine.hasOption('c')) {
            file = cmdLine.getOptionValue('c');
            if (!StringUtil.isNullOrEmpty(file)) {
                try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                    properties.load(in);
                }
            }
        }

        final BrokerConfig config = BrokerConfig.exchange(properties);
        checkAndPrintConfig(config);

        final BrokerServer server = new BrokerServer(config);

        Runtime.getRuntime().addShutdownHook(new ShutdownHook<>(() -> {
            server.shutdownGracefully();
            return  null;
        }));

        return server;
    }

    private static CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) throws ParseException {
        return parser.parse(options, args);
    }

    private static Options buildCommandOptions() {
        final Options options = new Options();
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static void checkAndPrintConfig(BrokerConfig config) {
        final Method[] methods = BrokerConfig.class.getDeclaredMethods();
        StringBuilder sb = new StringBuilder("Print the broker startup options: \n");
        String option;

        for (Method method : methods) {
            final String name = method.getName();
            if (name.startsWith("obtain")) {
                option = name.substring(6);
                checkReturnType(method, config, sb, option);
            }

            if (name.startsWith("is")) {
                option = name.substring(2);
                checkReturnType(method, config, sb, option);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info(sb.toString());
        }
    }

    private static void checkReturnType(Method method, BrokerConfig config, StringBuilder sb, String name) {
        String type = method.getReturnType().getSimpleName();
        Object invoke;
        try {
            switch (type) {
                case "int", "Integer" -> invoke = TypeUtil.object2Int(method.invoke(config));
                case "long", "Long" -> invoke = TypeUtil.object2Long(method.invoke(config));
                case "double", "Double" -> invoke = TypeUtil.object2Double(method.invoke(config));
                case "float", "Float" -> invoke = TypeUtil.object2Float(method.invoke(config));
                case "boolean", "Boolean" -> invoke = TypeUtil.object2Boolean(method.invoke(config));
                case "String" -> invoke = TypeUtil.object2String(method.invoke(config));
                default -> throw new OperationNotSupportedException("Not support type");
            }
            sb.append(String.format("\t%s=%s", name, invoke)).append("\n");
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to check config type, type:%s name:%s error:%s", type, name, e));
        }
    }
}
