package org.ostara.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.ostara.client.internal.Client;

public interface Command {
    String name();
    String description();
    Options buildOptions(final Options options);

    void execute(final CommandLine commandLine, final Options options, Client client) throws Exception;
}
