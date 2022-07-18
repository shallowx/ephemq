package org.shallow.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.shallow.ShallowClient;
import org.shallow.SubCommand;

public class DeleteTopicCommand implements SubCommand {
    @Override
    public String name() {
        return "deleteTopic";
    }

    @Override
    public String desc() {
        return "delete the topic";
    }

    @Override
    public Options buildOptions(Options options) {
        return null;
    }

    @Override
    public void execute(CommandLine cmdLine, Options options, ShallowClient client) throws Exception {

    }
}
