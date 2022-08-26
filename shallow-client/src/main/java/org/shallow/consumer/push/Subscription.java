package org.shallow.consumer.push;

public class Subscription {

    private final int epoch;
    private final long index;
    private final String queue;
    private final int ledger;

    public Subscription(int epoch, long index, String queue, int ledger) {
        this.epoch = epoch;
        this.index = index;
        this.queue = queue;
        this.ledger = ledger;
    }

    public int getEpoch() {
        return epoch;
    }

    public long getIndex() {
        return index;
    }

    public String getQueue() {
        return queue;
    }

    public int getLedger() {
        return ledger;
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "epoch=" + epoch +
                ", index=" + index +
                ", queue='" + queue + '\'' +
                ", ledger=" + ledger +
                '}';
    }
}
