package org.ostara.parser;

import io.netty.util.internal.StringUtil;
import java.io.File;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ConfigurableArgumentsRunListener implements ApplicationRunListener {
    private static final int NOT_FOUND = -1;
    public static final char EXTENSION_SEPARATOR = '.';
    private static final char UNIX_NAME_SEPARATOR = '/';
    private static final char WINDOWS_NAME_SEPARATOR = '\\';
    private final String[] args;

    public ConfigurableArgumentsRunListener(String[] args) {
        this.args = args;
    }

    @Override
    public ApplicationArguments startUp() throws Exception {
        return argumentsPrepared(args);
    }

    @Override
    public ApplicationArguments argumentsPrepared(String[] args) throws Exception {
        Options options = buildCommandOptions();
        CommandLine cmdLine = parseCmdLine(args, options, new DefaultParser());

        Properties properties = new Properties();
        if (cmdLine.hasOption('c')) {
            String file = cmdLine.getOptionValue('c');
            if (!StringUtil.isNullOrEmpty(file)) {
                String extension = getExtension(file);
                ResourceLoader propertySourceLoader = propertySourceLoaderBeanFactory(extension);
                properties = propertySourceLoader.load(file);
            }
        }
        return new DefaultApplicationArguments(properties);
    }

    private ResourceLoader propertySourceLoaderBeanFactory(String type) {
        switch (type) {
            case "properties" -> {
                return new PropertiesResourceLoader();
            }
            case "yaml", "yml" -> {
                return new YamlResourceLoader();
            }
            default -> {
                throw new RuntimeException(String.format("Not supported file type<%s>", type));
            }
        }
    }

    private String getExtension(final String fileName) throws IllegalArgumentException {
        if (fileName == null) {
            return null;
        }
        final int index = indexOfExtension(fileName);
        if (index == NOT_FOUND) {
            return StringUtil.EMPTY_STRING;
        }
        return fileName.substring(index + 1);
    }

    private int indexOfExtension(final String fileName) throws IllegalArgumentException {
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

    private boolean isSystemWindows() {
        return File.separatorChar == WINDOWS_NAME_SEPARATOR;
    }

    private int getAdsCriticalOffset(final String fileName) {
        final int suffix = fileName.lastIndexOf(File.separatorChar);
        final int osSuffix = fileName.lastIndexOf(flipSeparator());
        if (suffix == NOT_FOUND) {
            if (osSuffix == NOT_FOUND) {
                return 0;
            }
            return osSuffix + 1;
        }
        if (osSuffix == -1) {
            return suffix + 1;
        }
        return StrictMath.max(suffix, osSuffix) + 1;
    }

    private char flipSeparator() {
        if (File.separatorChar == UNIX_NAME_SEPARATOR) {
            return UNIX_NAME_SEPARATOR;
        }
        if (File.separatorChar == WINDOWS_NAME_SEPARATOR) {
            return WINDOWS_NAME_SEPARATOR;
        }
        throw new IllegalArgumentException(String.valueOf(File.separatorChar));
    }

    private int indexOfLastSeparator(final String fileName) {
        if (fileName == null) {
            return NOT_FOUND;
        }
        final int lastUnixPos = fileName.lastIndexOf(UNIX_NAME_SEPARATOR);
        final int lastWindowsPos = fileName.lastIndexOf(WINDOWS_NAME_SEPARATOR);
        return Math.max(lastUnixPos, lastWindowsPos);
    }

    private CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) throws ParseException {
        return parser.parse(options, args);
    }

    private Options buildCommandOptions() {
        Options options = new Options();
        Option opt = new Option("c", "configFile", true, "config file(properties / yaml)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
