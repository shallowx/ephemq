package org.shallow;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.thread.ShutdownHook;
import org.shallow.nameserver.NameCoreServer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

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

        NameCoreServer server = new NameCoreServer(config);

        Runtime.getRuntime().addShutdownHook(new ShutdownHook<>("broker server", () -> {
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
}
