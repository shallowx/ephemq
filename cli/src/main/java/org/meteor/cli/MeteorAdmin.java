package org.meteor.cli;

import org.apache.commons.cli.*;
import org.meteor.cli.topic.TopicCreatedCommand;
import org.meteor.cli.cluster.ClusterListCommand;
import org.meteor.cli.topic.TopicDeletedCommand;
import org.meteor.cli.topic.TopicListCommand;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.ClientListener;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MeteorAdmin {
    private static final List<Command> commands = new ArrayList<>();

    public static void main(String[] args) {
        main0(args);
    }

    private static void main0(String[] args) {
        try {
            initCommand();
            switch (args.length) {
                case 0 -> printHelp();
                case 2 -> {
                    if (args[0].equals("help")) {
                        Command cmd = getCommand(args[1]);
                        if (cmd != null) {
                            Options options = buildOptions();
                            options = cmd.buildOptions(options);
                            if (options != null) {
                                printCmdHelp("smartAdmin" + cmd.name(), options);
                                return;
                            }
                            System.out.printf("%s [%s] ERROR %s - The command does not exists, cname=%s \n",
                                    newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[1]);
                        }
                    }
                }
                case 1 -> {
                }
                default -> {
                    Command cmd = getCommand(args[0]);
                    if (cmd != null) {
                        String[] cmdArgs = parseCmdArgs(args);
                        Options options = buildOptions();
                        CommandLine cmdLine = parseCommandLine(cmdArgs, cmd.buildOptions(options));
                        if (cmdLine == null) {
                            System.out.printf("%s [%s] ERROR %s - The command does not exists, cname=%s \n",
                                    newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[0]);
                            return;
                        }

                        if (cmdLine.hasOption('c')) {
                            String address = cmdLine.getOptionValue('b');
                            ClientConfig config = new ClientConfig();
                            config.setBootstrapAddresses(new ArrayList<>() {
                                {
                                    add(address);
                                }
                            });

                            Client client = new Client("cmdLine-client", config, new ClientListener() {
                            });
                            try {
                                client.start();
                                cmd.execute(cmdLine, options, client);
                            } catch (Exception e) {
                                client.close();
                                throw e;
                            }
                        }
                        return;
                    }
                    System.out.printf("%s [%s] INFO %s - The command does not exists, cname=%s \n",
                            newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[0]);
                }
            }
        } catch (Throwable t) {
            System.out.printf("%s [%s] INFO %s - The command does not exists, cname=%s \n",
                    newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[0]);
            System.exit(-1);
        }
    }

    private static CommandLine parseCommandLine(String[] args, Options options) throws Exception {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        DefaultParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static String[] parseCmdArgs(String[] args) {
        int length = args.length;
        if (length > 1) {
            String[] ret = new String[length - 1];
            System.arraycopy(args, 1, ret, 0, length);
            return ret;
        }
        return null;
    }

    private static String newDate() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
        return format.format(new Date());
    }

    private static void printCmdHelp(String help, Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(help, options, true);
    }

    private static Options buildOptions() {
        Option option = new Option("h", "help", false, "Print help");
        option.setRequired(false);
        Options options = new Options();
        options.addOption(option);

        return options;
    }

    private static void initCommand() {
        Command clientCommand = new TopicListCommand();
        Command clusterCommand = new ClusterListCommand();
        Command topicCreatedCommand = new TopicCreatedCommand();
        Command topicDeletedCommand = new TopicDeletedCommand();
        commands.add(clientCommand);
        commands.add(clusterCommand);
        commands.add(topicCreatedCommand);
        commands.add(topicDeletedCommand);
    }

    private static Command getCommand(String name) {
        for (Command cmd : commands) {
            if (cmd.name().equals(name)) {
                return cmd;
            }
        }
        return null;
    }

    private static void printHelp() {
        System.out.printf("the most commonly used commands are:%n");
        for (Command cmd : commands) {
            System.out.printf("  %s-20s %s%n", cmd.name(), cmd.description());
        }
        System.out.printf("%n see 'smartAdmin help <command>' for more information on a specific command.%n");
    }
}
