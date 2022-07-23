package org.shallow;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.reflections.Reflections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.shallow.CmdToolUtil.*;
import static org.shallow.ObjectUtil.checkNotNull;

public class ShallowCmdLineTool {

    private static Client client;

    public static void main(String[] args) {
        main0(args);
    }

    private static void main0(String[] args) {
        try {
           final List<SubCommand> commands = initCommand();

        } catch (Exception e) {
            System.err.printf("%s [%s] ERROR %s - %s \n", newDate(), currentThread(), className(ShallowCmdLineTool.class), e);
        }
    }

    private static List<SubCommand> initCommand() throws Exception {
        final List<SubCommand> commands = new LinkedList<>();

        String packageName = SubCommand.class.getPackageName();
        Reflections f = new Reflections(packageName);
        Set<Class<? extends  SubCommand>> classes = f.getSubTypesOf(SubCommand.class);
        for (Class<?> c : classes) {
            Object bean = c.getDeclaredConstructor().newInstance();
            if (bean instanceof SubCommand) {
                addCommand((SubCommand) bean, commands);
            }
        }
        return commands;
    }

    private static void addCommand(SubCommand command, List<SubCommand> commands) {
        commands.add(command);
    }

    private SubCommand acquireCommand(final String name, List<SubCommand> commands) {
        checkNotNull(name, "command is null");
        if (commands.isEmpty()) {
            return null;
        }

        for (SubCommand command : commands) {
            if (command.name().equalsIgnoreCase(name)) {
                return command;
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
