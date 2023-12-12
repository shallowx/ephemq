package org.meteor.ledger;

import org.meteor.client.internal.ClientChannel;

public class Migration {
    private final int ledger;
    private final ClientChannel channel;

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
