package org.leopard;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.thread.ShutdownHook;
import org.leopard.nameserver.NameCoreServer;

import javax.naming.OperationNotSupportedException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.leopard.common.util.TypeUtils.*;
import static org.leopard.common.util.TypeUtils.object2String;

public class Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Server.class);

    private static final int NOT_FOUND = -1;
    private static final String EMPTY_STRING = "";
    public static final char EXTENSION_SEPARATOR = '.';
    private static final char UNIX_NAME_SEPARATOR = '/';
    private static final char WINDOWS_NAME_SEPARATOR = '\\';

    public static void main(String[] args ) {
        try {
            run(args);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start server failed", e);
            }
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        NameserverConfig config = argumentsPrepared(args);
        checkAndPrintConfig(config);

        NameCoreServer server = new NameCoreServer(config);

        Runtime.getRuntime().addShutdownHook(new ShutdownHook<>("Name server", () -> {
            server.shutdownGracefully();
            return  null;
        }));

        server.start();
    }

    private static NameserverConfig argumentsPrepared(String[] args) throws Exception {
        Options options = buildCommandOptions();
        CommandLine cmdLine = parseCmdLine(args, options, new DefaultParser());

        Properties properties = new Properties();
        if (cmdLine.hasOption('c')) {
            String file = cmdLine.getOptionValue('c');
            if (!StringUtil.isNullOrEmpty(file)) {
                String extension = getExtension(file);
                try (InputStream in = new BufferedInputStream(new FileInputStream(extension))) {
                    properties.load(in);
                }
            }
        }
        return NameserverConfig.exchange(properties);
    }


    private static String getExtension(final String fileName) throws IllegalArgumentException {
        if (fileName == null) {
            return null;
        }
        final int index = indexOfExtension(fileName);
        if (index == NOT_FOUND) {
            return EMPTY_STRING;
        }
        return fileName.substring(index + 1);
    }

    private static int indexOfExtension(final String fileName) throws IllegalArgumentException {
        if (fileName == null) {
            return NOT_FOUND;
        }

        if (isSystemWindows()) {
            final int offset = fileName.indexOf(':', getAdsCriticalOffset(fileName));
            if (offset != -1) {
                throw new IllegalArgumentException("NTFS ADS separator (':') in file name is forbidden");
            }
        }

        final int extensionPos = fileName.lastIndexOf(EXTENSION_SEPARATOR);
        final int lastSeparator = indexOfLastSeparator(fileName);
        return lastSeparator > extensionPos ? NOT_FOUND : extensionPos;
    }

    private static boolean isSystemWindows() {
        return File.separatorChar == WINDOWS_NAME_SEPARATOR;
    }

    private static int getAdsCriticalOffset(final String fileName) {
        final int offset1 = fileName.lastIndexOf(File.separatorChar);
        final int offset2 = fileName.lastIndexOf(flipSeparator());
        if (offset1 == -1) {
            if (offset2 == -1) {
                return 0;
            }
            return offset2 + 1;
        }
        if (offset2 == -1) {
            return offset1 + 1;
        }
        return StrictMath.max(offset1, offset2) + 1;
    }

    private static char flipSeparator() {
        if (File.separatorChar == UNIX_NAME_SEPARATOR) {
            return UNIX_NAME_SEPARATOR;
        }
        if (File.separatorChar == WINDOWS_NAME_SEPARATOR) {
            return WINDOWS_NAME_SEPARATOR;
        }
        throw new IllegalArgumentException(String.valueOf(File.separatorChar));
    }

    private static int indexOfLastSeparator(final String fileName) {
        if (fileName == null) {
            return NOT_FOUND;
        }
        final int lastUnixPos = fileName.lastIndexOf(UNIX_NAME_SEPARATOR);
        final int lastWindowsPos = fileName.lastIndexOf(WINDOWS_NAME_SEPARATOR);
        return Math.max(lastUnixPos, lastWindowsPos);
    }

    private static CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) throws ParseException {
        return parser.parse(options, args);
    }

    private static Options buildCommandOptions() {
        Options options = new Options();
        Option opt = new Option("c", "configFile", true, "Nameserver config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static void checkAndPrintConfig(NameserverConfig config) {
        Method[] methods = NameserverConfig.class.getDeclaredMethods();
        StringBuilder sb = new StringBuilder("Print the broker startup options: \n");
        String option;

        for (Method method : methods) {
            final String name = method.getName();
            if (name.startsWith("get")) {
                option = name.substring(3);
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


    private static void checkReturnType(Method method, NameserverConfig config, StringBuilder sb, String name) {
        String type = method.getReturnType().getSimpleName();
        Object invoke;
        try {
            switch (type) {
                case "int", "Integer" -> invoke = object2Int(method.invoke(config));
                case "long", "Long" -> invoke = object2Long(method.invoke(config));
                case "double", "Double" -> invoke = object2Double(method.invoke(config));
                case "float", "Float" -> invoke = object2Float(method.invoke(config));
                case "boolean", "Boolean" -> invoke = object2Boolean(method.invoke(config));
                case "String" -> invoke = object2String(method.invoke(config));
                default -> throw new OperationNotSupportedException("Not support type");
            }
            sb.append(String.format("\t%s=%s", name, invoke)).append("\n");
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to check config type, type:%s name:%s error:%s", type, name, e));
        }
    }
}
