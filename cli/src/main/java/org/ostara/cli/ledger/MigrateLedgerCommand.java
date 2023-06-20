package org.ostara.cli.ledger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.ostara.cli.Command;
import org.ostara.client.internal.Client;

public class MigrateLedgerCommand implements Command {
    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public Options buildOptions(Options options) {
        return null;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, Client client) throws Exception {

    }
}
