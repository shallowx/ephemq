package org.shallow;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurableArgumentsRunListener implements ApplicationRunListener{

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConfigurableArgumentsRunListener.class);

    private final String[] args;

    public ConfigurableArgumentsRunListener(String[] args) {
        this.args = args;
    }

    @Override
    public ApplicationArguments starting() throws Exception {
        return argumentsPrepared(args);
    }

    @Override
    public ApplicationArguments argumentsPrepared(String[] args) throws Exception {
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
        return new DefaultApplicationArguments(properties);
    }

    private static CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) throws ParseException {
        return parser.parse(options, args);
    }

    private static Options buildCommandOptions() {
        Options options = new Options();
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
