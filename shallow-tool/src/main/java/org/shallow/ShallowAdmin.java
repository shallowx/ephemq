package org.shallow;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ShallowAdmin {

    private ShallowClient client;

    public static void main(String[] args) {

    }

    private static void main0(String[] args) {
        List<SubCommand> commands = new LinkedList<>();
    }

    private static SubCommand getCmdLine(final String name, List<SubCommand> commands) {
        Objects.requireNonNull(name, "command name cannot be empty");
        for (SubCommand cmd : commands) {
            if (name.equals(cmd.name())) {
                return cmd;
            }
        }
        return null;
    }

    private static Options buildOptions(final Options options) {
        Option option = new Option("h", "help", false, "print help");
        option.setRequired(false);
        options.addOption(option);

        return options;
    }

    private static void printCmdLineHelp(final String name, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(115);
        hf.printHelp(name, options, true);
    }

    private static void printHelp(List<SubCommand> commands) {
        System.out.printf("the most commonly used shallow commands are:%n");
        for (SubCommand cmd : commands) {
            System.out.printf("   %-20s %s%n", cmd.name(), cmd.desc());
        }

        System.out.printf("%n see 'shallow help <command>' for more information on a specific command.%n");
    }
}
