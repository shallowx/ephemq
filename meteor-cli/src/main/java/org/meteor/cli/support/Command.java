package org.meteor.cli.support;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.meteor.client.core.Client;

/**
 * The Command interface defines the structure for various command implementations
 * that can be used within the application. Each Command is expected to have a name,
 * a description, and a set of options that can be built and executed.
 */
public interface Command {
    /**
     * Returns the name of the command.
     *
     * @return the name of the command
     */
    String name();

    /**
     * Provides a brief description of the command.
     *
     * @return a string containing a short description of the command.
     */
    String description();

    /**
     * Builds and returns a set of command line options for a specific command.
     * The provided options object is used as the base and additional options
     * specific to the command are added to it.
     *
     * @param options the initial set of command line options to be built upon
     * @return the complete set of command line options for the command
     */
    Options buildOptions(final Options options);

    /**
     * Executes the command with the provided command line arguments, options, and client.
     *
     * @param commandLine the command line arguments passed to the command
     * @param options     the configuration options available for the command
     * @param client      the client that facilitates the execution of this command
     * @throws Exception if an error occurs during command execution
     */
    void execute(final CommandLine commandLine, final Options options, Client client) throws Exception;

    /**
     * Retrieves the current time formatted as "HH:mm:ss.SSS" in the UTC +08:00 time zone.
     *
     * @return a string representing the current time in the "HH:mm:ss.SSS" format.
     */
    default String currentTime() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now(ZoneId.of("UTC +08:00")));
    }
}
