package org.meteor.cli;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.meteor.cli.cluster.ClusterListCommand;
import org.meteor.cli.ledger.MigrateLedgerCommand;
import org.meteor.cli.ledger.MigrateLedgerPlanCommand;
import org.meteor.cli.support.Command;
import org.meteor.cli.support.MeteorHelpFormatter;
import org.meteor.cli.topic.TopicCreatedCommand;
import org.meteor.cli.topic.TopicDeletedCommand;
import org.meteor.cli.topic.TopicListCommand;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;

public class MeteorAdmin {

    public static void main(String[] args) {
        main0(args);
    }

    private static void main0(String[] args) {
        try {
            initCommand();
            switch (args.length) {
                case 0, 1 -> printHelp();
                case 2 -> {
                    if (args[0].equals("help")) {
                        Command cmd = getCommand(args[1]);
                        if (cmd != null) {
                            Options options = buildOptions();
                            options = cmd.buildOptions(options);
                            if (options != null) {
                                printCmdHelp(STR."Meteor-cli admin \{cmd.name()}", options);
                                return;
                            }
                            System.err.println(STR."\{newDate()} [\{Thread.currentThread()
                                    .getName()}] ERROR \{MeteorAdmin.class.getName()} - The command[:\{args[1]}] does not exists");
                        }
                    }
                }
                default -> {
                    Command cmd = getCommand(args[0]);
                    if (cmd != null) {
                        String[] cmdArgs = parseCmdArgs(args);
                        Options options = buildOptions();
                        CommandLine cmdLine = parseCommandLine(cmdArgs, cmd.buildOptions(options));
                        if (cmdLine == null) {
                            System.err.println(STR."\{newDate()} [\{Thread.currentThread()
                                    .getName()}] ERROR \{MeteorAdmin.class.getName()} - The command[:\{args[0]}] does not exists");
                            return;
                        }

                        if (cmdLine.hasOption('b')) {
                            String address = cmdLine.getOptionValue('b');
                            ClientConfig config = new ClientConfig();
                            config.setBootstrapAddresses(new ArrayList<>() {
                                {
                                    add(address);
                                }
                            });

                            Client client = new Client("meteor-cli-client", config, new CombineListener() {
                            });
                            try {
                                client.start();
                                cmd.execute(cmdLine, options, client);
                            } catch (Exception e) {
                                client.close();
                                throw e;
                            }
                        }
                    } else {
                        System.err.println(STR."\{newDate()} [\{Thread.currentThread()
                                .getName()}] INFO \{MeteorAdmin.class.getName()} - The command[:\{args[0]}] does not exists");
                    }
                }
            }
        } catch (Throwable t) {
            System.err.println(STR."\{newDate()} [\{Thread.currentThread()
                    .getName()}] ERROR \{MeteorAdmin.class.getName()} - \{t.getMessage()}");
            System.exit(-1);
        }
        System.exit(0);
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
            System.arraycopy(args, 1, ret, 0, length - 1);
            return ret;
        }
        return null;
    }

    private static String newDate() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now());
    }

    private static void printCmdHelp(String help, Options options) {
        MeteorHelpFormatter hf = new MeteorHelpFormatter();
        hf.setWidth(110);
        hf.printHelp(help, options, false);
    }

    private static Options buildOptions() {
        Option option = new Option("h", "help", false, "Show this help message");
        option.setRequired(false);
        Options options = new Options();
        options.addOption(option);

        return options;
    }

    private static final Map<String, Command> commands = new HashMap<>(8);
    private static void initCommand() {
        Command clientCmd = new TopicListCommand();
        Command clusterCmd = new ClusterListCommand();
        Command topicCreatedCmd = new TopicCreatedCommand();
        Command topicDeletedCmd = new TopicDeletedCommand();
        Command planCmd = new MigrateLedgerPlanCommand();
        Command migrateCmd = new MigrateLedgerCommand();

        commands.put(clientCmd.name(), clientCmd);
        commands.put(clusterCmd.name(), clusterCmd);
        commands.put(topicCreatedCmd.name(), topicCreatedCmd);
        commands.put(topicDeletedCmd.name(), topicDeletedCmd);
        commands.put(planCmd.name(), planCmd);
        commands.put(migrateCmd.name(), migrateCmd);
    }

    private static Command getCommand(String name) {
        return commands.get(name);
    }

    private static void printHelp() {
        System.out.printf("Common Commands:%n");
        for (Command cmd : commands.values()) {
            System.out.printf(" %-20s %s%n", cmd.name(), cmd.description());
        }
        System.out.printf("%n Run 'COMMAND --help' for more information on a command..%n");
    }
}
