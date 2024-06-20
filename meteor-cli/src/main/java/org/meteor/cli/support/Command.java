package org.meteor.cli.support;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.meteor.client.core.Client;

public interface Command {
    String name();
    String description();
    Options buildOptions(final Options options);
    void execute(final CommandLine commandLine, final Options options, Client client) throws Exception;
    default String newDate() {
        return DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(LocalTime.now());
    }
}
