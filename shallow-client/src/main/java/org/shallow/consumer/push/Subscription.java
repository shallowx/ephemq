package org.shallow.consumer.push;

public record Subscription(int epoch, long index, String queue, int ledger) {

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
