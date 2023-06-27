package org.ostara.cli.ledger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;

@SuppressWarnings("all")
public class MigrateLedgerPlanCommand implements Command {
    @Override
    public String name() {
        return "migratePlan";
    }

    @Override
    public String description() {
        return "create migrate plan file";
    }

    @Override
    public Options buildOptions(Options options) {
        Option brokerOpt = new Option("b", "broker address", true, "which broker server");
        brokerOpt.setRequired(true);
        options.addOption(brokerOpt);

        Option partitionOpt = new Option("o", "original broker", true, "original broker");
        partitionOpt.setRequired(true);
        options.addOption(partitionOpt);

        Option replicaOpt = new Option("e", "exclude broker", true, "exclude broker");
        partitionOpt.setRequired(true);
        options.addOption(replicaOpt);

        Option configOpt = new Option("v", "verify completed", true, "verify completed");
        partitionOpt.setRequired(true);
        options.addOption(configOpt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {

    }
}
