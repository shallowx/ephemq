package org.shallow;

import io.netty.util.internal.StringUtil;
import org.apache.commons.cli.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Shallow {
    public static void main( String[] args ) {
        System.out.println( "Hello World!" );
    }

    private static void start() {
    }

    private static void createShallowServer(String[] args) throws Exception {
        Options options = buildCommandOptions();
        CommandLine cmdLine = parseCmdLine(args, options, new PosixParser());

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
}
