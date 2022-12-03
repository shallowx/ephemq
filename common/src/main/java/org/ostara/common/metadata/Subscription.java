package org.ostara.common.metadata;

public class Subscription {

    private int epoch;
    private long index;
    private String queue;
    private int ledger;
    private short version;

    public void setLedger(int ledger) {
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

    public short getVersion() {
        return version;
    }

    public static SubscriptionBuilder newBuilder() {
        return new SubscriptionBuilder();
    }

    public static class SubscriptionBuilder {
        private int epoch;
        private long index;
        private String queue;
        private int ledger;
        private short version;

        public SubscriptionBuilder epoch(int epoch) {
            this.epoch = epoch;
            return this;
        }

        public SubscriptionBuilder index(long index) {
            this.index = index;
            return this;
        }

        public SubscriptionBuilder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public SubscriptionBuilder ledger(int ledger) {
            this.ledger = ledger;
            return this;
        }

        public SubscriptionBuilder version(short version) {
            this.version = version;
            return this;
        }

        public Subscription build() {
            Subscription subscription = new Subscription();

            subscription.ledger = this.ledger;
            subscription.queue = this.queue;
            subscription.version = this.version;
            subscription.index = this.index;
            subscription.epoch = this.epoch;

            return subscription;
        }
    }


    @Override
    public String toString() {
        return "Subscription{" +
                "epoch=" + epoch +
                ", index=" + index +
                ", queue='" + queue + '\'' +
                ", ledger=" + ledger +
                ", version=" + version +
                '}';
    }
}
