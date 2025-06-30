package org.meteor.cli;

import org.apache.commons.cli.*;
import org.meteor.cli.cluster.ClusterListCommand;
import org.meteor.cli.ledger.MigrateLedgerCommand;
import org.meteor.cli.ledger.MigrateLedgerPlanCommand;
import org.meteor.cli.support.Command;
import org.meteor.cli.topic.TopicCreatedCommand;
import org.meteor.cli.topic.TopicDeletedCommand;
import org.meteor.cli.topic.TopicListCommand;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The MeteorAdmin class serves as the main entry point for the Meteor CLI application.
 * It supports various administrative commands related to Meteor, such as listing topics,
 * clusters, and performing operations like creating and deleting topics.
 * <p>
 * The primary responsibilities of this class include:
 * - Initializing available commands
 * - Parsing the command-line arguments
 * - Executing the specified command with provided options
 */
public class MeteorAdmin {

    public static void main(String[] args) {
        main0(args);
    }

    /**
     * Executes the core logic of the MeteorAdmin command-line tool.
     * Depending on the provided arguments, it initializes commands, prints help,
     * parses command-line arguments, and executes the intended commands.
     *
     * @param args the command-line arguments passed to the program
     */
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
                                printCmdHelp(String.format("Meteor-cli admin %s", cmd.name()), options);
                                return;
                            }
                            System.out.printf("%s %s INFO - %s - The command[%s] does not exists", newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[1]);                        }
                    }
                }
                default -> {
                    Command cmd = getCommand(args[0]);
                    if (cmd != null) {
                        String[] cmdArgs = parseCmdArgs(args);
                        Options options = buildOptions();
                        CommandLine cmdLine = parseCommandLine(cmdArgs, cmd.buildOptions(options));
                        if (cmdLine == null) {
                            System.out.printf("%s %s INFO - %s - The command[%s] does not exists", newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[0]);
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
                        System.out.printf("%s %s INFO - %s - The command[%s] does not exists", newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), args[0]);
                    }
                }
            }
        } catch (Throwable t) {
            System.out.printf("%s %s ERROR - %s - %s", newDate(), Thread.currentThread().getName(), MeteorAdmin.class.getName(), t.getMessage());            System.exit(-1);
        }
        System.exit(0);
    }

    /**
     * Parses the command line arguments and returns a CommandLine object.
     *
     * @param args the command line arguments to parse
     * @param options the options that define the possible arguments
     * @return a CommandLine object representing the parsed arguments
     * @throws Exception if an error occurs during parsing
     */
    private static CommandLine parseCommandLine(String[] args, Options options) throws Exception {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        DefaultParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    /**
     * Parses the given command line arguments and returns a new array without the first element.
     *
     * @param args the array of command line arguments
     * @return a new array containing the command line arguments excluding the first one, or null if the input array has one or zero elements
     */
    private static String[] parseCmdArgs(String[] args) {
        int length = args.length;
        if (length > 1) {
            String[] ret = new String[length - 1];
            System.arraycopy(args, 1, ret, 0, length - 1);
            return ret;
        }
        return null;
    }

    /**
     * Generates a string representing the current time formatted as "HH:mm:ss.SSS".
     *
     * @return the current time as a formatted string
     */
    private static String newDate() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now());
    }

    /**
     * Prints the command help information using the provided help message and command line options.
     *
     * @param help the help message to be displayed
     * @param options the command line options associated with the command
     */
    private static void printCmdHelp(String help, Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(help, options, false);
    }

    /**
     * Builds and returns the command line options for the application.
     *
     * @return the command line options for the application
     */
    private static Options buildOptions() {
        Option option = new Option("h", "help", false, "Show this help message");
        option.setRequired(false);
        Options options = new Options();
        options.addOption(option);

        return options;
    }

    /**
     * A static map to hold the available commands in the application.
     * Key is the command name, and value is the corresponding Command object.
     */
    private static final Map<String, Command> commands = new HashMap<>(8);

    /**
     * Initializes command objects and adds them to the commands map.
     * <p>
     * This method creates instances of various command classes such as TopicListCommand,
     * ClusterListCommand, TopicCreatedCommand, TopicDeletedCommand, MigrateLedgerPlanCommand,
     * and MigrateLedgerCommand. These command objects are then added to a map called commands,
     * with their respective command names as the keys.
     */
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

    /**
     * Retrieves the Command object associated with the given name.
     *
     * @param name the name of the command to retrieve
     * @return the Command object associated with the name, or null if no command is found
     */
    private static Command getCommand(String name) {
        return commands.get(name);
    }

    /**
     * Prints the list of common commands along with their descriptions.
     * This method formats and outputs help information for each registered command.
     * Each command is printed with its name and a brief description.
     * The message informs users to run 'COMMAND --help' for detailed information on a specific command.
     */
    private static void printHelp() {
        System.out.printf("Common Commands:%n");
        for (Command cmd : commands.values()) {
            System.out.printf(" %-20s %s%n", cmd.name(), cmd.description());
        }
        System.out.printf("%n Run 'COMMAND --help' for more information on a command..%n");
    }
}
