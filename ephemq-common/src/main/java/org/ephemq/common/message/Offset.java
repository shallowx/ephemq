package org.ephemq.common.message;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Represents a point in a stream defined by an epoch and an index.
 * This class is immutable and implements the {@code Comparable} interface
 * to allow ordering based on the combination of its epoch and index.
 */
@Immutable
public class Offset implements Comparable<Offset> {
    /**
     * The epoch number of the stream, used to define the timeline of events within the stream.
     * It is a non-negative integer that is part of the unique identifier for a point in the stream.
     */
    private final int epoch;
    /**
     * Represents the index within the current epoch of the stream.
     * This value is a long integer to support very large streams.
     */
    private final long index;

    /**
     * Constructs an {@code Offset} instance with the specified epoch and index.
     *
     * @param epoch The epoch value of the offset. Must be non-negative.
     * @param index The index value of the offset.
     */
    public Offset(int epoch, long index) {
        this.epoch = epoch;
        this.index = index;
    }

    /**
     * Creates a new {@code Offset} instance with the specified epoch and index.
     *
     * @param epoch the epoch of the new {@code Offset}. If the epoch is negative, the method returns {@code null}.
     * @param index the index within the specified epoch for the new {@code Offset}
     * @return a new {@code Offset} instance with the provided epoch and index, or {@code null} if the epoch is negative
     */
    public static Offset of(int epoch, long index) {
        return epoch < 0 ? null : new Offset(epoch, index);
    }

    /**
     * Determines whether this Offset instance precedes the specified Offset.
     *
     * @param o the Offset to compare with this instance
     * @return true if this Offset precedes the specified Offset; false otherwise
     */
    public boolean before(Offset o) {
        return this.compareTo(o) < 0;
    }

    /**
     * Determines if this Offset occurs after the specified Offset.
     *
     * @param o the Offset to compare against
     * @return true if this Offset is after the specified Offset; false otherwise
     */
    public boolean after(Offset o) {
        return this.compareTo(o) > 0;
    }

    /**
     * Compares this Offset with the specified Offset for order.
     * Returns a negative integer, zero, or a positive integer
     * as this Offset is less than, equal to, or greater than the specified Offset.
     *
     * @param o the Offset to be compared
     * @return a negative integer, zero, or a positive integer as this Offset
     * is less than, equal to, or greater than the specified Offset
     */
    @Override
    public int compareTo(@Nonnull Offset o) {
        if (this.epoch == o.epoch) {
            return (int) (this.index - o.index);
        }
        return this.epoch - o.epoch;
    }

    /**
     * Retrieves the epoch component of this Offset.
     *
     * @return the epoch value representing the version of the stream.
     */
    public int getEpoch() {
        return epoch;
    }

    /**
     * Retrieves the index that represents a point in the stream.
     *
     * @return the index of this {@code Offset}
     */
    public long getIndex() {
        return index;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param o the reference object with which to compare.
     * @return true if this object is the same as the obj argument, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Offset offset = (Offset) o;
        return epoch == offset.epoch && index == offset.index;
    }

    /**
     * Computes the hash code for the Offset object based on its epoch and index.
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hash(epoch, index);
    }

    /**
     * Returns a string representation of the Offset object.
     *
     * @return a string in the format (epoch=<epoch>, index=<index>) where
     *         <epoch> and <index> are the respective values of the Offset instance.
     */
    @Override
    public String toString() {
        return "Offset (epoch=%d, index=%d)".formatted(epoch, index);
    }
}
