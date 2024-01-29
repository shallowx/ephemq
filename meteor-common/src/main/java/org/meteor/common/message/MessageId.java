package org.meteor.common.message;

public record MessageId(int ledger, int epoch, long index) {

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
        return "MessageId{" +
                "ledger=" + ledger +
                ", epoch=" + epoch +
                ", index=" + index +
                '}';
    }
}
