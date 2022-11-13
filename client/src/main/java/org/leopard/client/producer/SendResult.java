package org.leopard.client.producer;

public class SendResult {

    private final int epoch;
    private final long index;
    private final int ledger;

    public SendResult(int epoch, long index, int ledger) {
        this.epoch = epoch;
        this.index = index;
        this.ledger = ledger;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "epoch=" + epoch +
                ", index=" + index +
                ", ledger=" + ledger +
                '}';
    }
}
