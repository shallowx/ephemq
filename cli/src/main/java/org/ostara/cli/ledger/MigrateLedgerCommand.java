package org.ostara.cli.ledger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;

@SuppressWarnings("all")
public class MigrateLedgerCommand implements Command {
    @Override
    public String name() {
        return "migrateLedger";
    }

    @Override
    public String description() {
        return "migrate ledger";
    }

    @Override
    public Options buildOptions(Options options) {
        Option bOpt = new Option("b", "broker address", true, "which broker server");
        bOpt.setRequired(true);
        options.addOption(bOpt);

        Option originalOpt = new Option("o", "original broker", true, "original broker");
        originalOpt.setRequired(true);
        options.addOption(originalOpt);

        Option topicOpt = new Option("t", "topic", true, "which topic name");
        topicOpt.setRequired(true);
        options.addOption(topicOpt);


        Option partitionOpt = new Option("p", "partition", true, "which partition id");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option explainOpt = new Option("e", "explain file", true, "explain file");
        explainOpt.setRequired(true);
        options.addOption(explainOpt);

        Option destOpt = new Option("d", "destination broker", true, "destination broker");
        destOpt.setRequired(true);
        options.addOption(destOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {

    }
}
