package org.ostara.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.ostara.client.internal.Client;

public class TopicCommand implements Command{
    @Override
    public String name() {
        return "createTopic";
    }

    @Override
    public String description() {
        return "create topic";
    }

    @Override
    public Options buildOptions(Options options) {
        // TODO
        return null;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {
        // TODO
    }
}
