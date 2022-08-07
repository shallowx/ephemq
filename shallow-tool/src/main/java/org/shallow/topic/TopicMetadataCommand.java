package org.shallow.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.shallow.Client;
import org.shallow.SubCommand;

public class TopicMetadataCommand implements SubCommand {
    @Override
    public String name() {
        return "TopicList";
    }

    @Override
    public String desc() {
        return "Query the topic metadata";
    }

    @Override
    public Options buildOptions(Options options) {
        return null;
    }

    @Override
    public void execute(CommandLine cmdLine, Options options, Client client) throws Exception {

    }
}
