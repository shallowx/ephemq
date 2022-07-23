package org.shallow.cluster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.shallow.Client;
import org.shallow.SubCommand;

public class ClusterCommand implements SubCommand {
    @Override
    public String name() {
        return "clusterList";
    }

    @Override
    public String desc() {
        return "fetch cluster metadata";
    }

    @Override
    public Options buildOptions(Options options) {
        return null;
    }

    @Override
    public void execute(CommandLine cmdLine, Options options, Client client) throws Exception {

    }
}
