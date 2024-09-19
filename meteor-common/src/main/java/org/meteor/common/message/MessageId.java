package org.meteor.common.message;

/**
 * The MessageId class represents a unique identifier for a message within a system.
 * It consists of three components: ledger, epoch, and index.
 * <p>
 * - ledger: An integer representing the ledger number.
 * - epoch: An integer representing the epoch number.
 * - index: A long integer representing the index number.
 * <p>
 * The MessageId class is used to track and identify messages, ensuring that
 * each message can be uniquely identified within a distributed system.
 * The class overrides the equals, hashCode, and toString methods to provide
 * appropriate comparison, hashing, and string representation functionalities.
 */
public record MessageId(int ledger, int epoch, long index) {

    /**
     * Checks if this MessageId is equal to another object.
     *
     * @param o the object to be compared with this MessageId.
     * @return true if the specified object is equal to this MessageId, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageId messageId = (MessageId) o;
        return ledger == messageId.ledger && epoch == messageId.epoch && index == messageId.index;
    }

    /**
     * Returns a hash code value for this MessageId. The hash code is computed
     * based on the values of the ledger, epoch, and index components.
     *
     * @return a hash code value for this MessageId
     */
    @Override
    public int hashCode() {
        return 31 * ((31 * ledger) + epoch) + (int) (index ^ (index >>> 32));
    }

    /**
     * Returns a string representation of the MessageId.
     *
     * @return a string in the format "(ledger=ledger, epoch=epoch, index=index)"
     */
    @Override
    public String toString() {
        return STR."(ledger=\{ledger}, epoch=\{epoch}, index=\{index})";
    }
}
