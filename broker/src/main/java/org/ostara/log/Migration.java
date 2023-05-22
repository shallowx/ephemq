package org.ostara.log;

import org.ostara.client.internal.ClientChannel;

public class Migration {
    private int ledger;
    private ClientChannel channel;

    public Migration(int ledger, ClientChannel channel) {
        this.ledger = ledger;
        this.channel = channel;
    }

    public int getLedger() {
        return ledger;
    }

    public ClientChannel getChannel() {
        return channel;
    }
}
