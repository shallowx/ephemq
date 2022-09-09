package org.shallow.consumer.push;

public record Subscription(int epoch, long index, String queue, int ledger, short version) {

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
