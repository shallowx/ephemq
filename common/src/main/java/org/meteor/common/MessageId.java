package org.meteor.common;

public class MessageId {
    private final int ledger;
    private final int epoch;
    private final long index;

    public MessageId(int ledger, int epoch, long index) {
        this.ledger = ledger;
        this.epoch = epoch;
        this.index = index;
    }

    public int ledger() {
        return ledger;
    }

    public int epoch() {
        return epoch;
    }

    public long index() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageId messageId = (MessageId) o;
        return ledger == messageId.ledger && epoch == messageId.epoch && index == messageId.index;
    }

    @Override
    public int hashCode() {
        return 31 * ((31 * ledger) + epoch) + (int) (index ^ (index >>> 32));
    }

    @Override
    public String toString() {
        return "message_id{" +
                "ledger=" + ledger +
                ", epoch=" + epoch +
                ", index=" + index +
                '}';
    }
}
