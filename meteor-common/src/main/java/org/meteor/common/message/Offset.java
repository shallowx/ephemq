package org.meteor.common.message;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
public class Offset implements Comparable<Offset> {

    private final int epoch;
    private final long index;

    public Offset(int epoch, long index) {
        this.epoch = epoch;
        this.index = index;
    }

    public static Offset of(int epoch, long index) {
        return epoch < 0 ? null : new Offset(epoch, index);
    }

    public boolean before(Offset o) {
        return this.compareTo(o) < 0;
    }

    public boolean after(Offset o) {
        return this.compareTo(o) > 0;
    }

    @Override
    public int compareTo(@Nonnull Offset o) {
        if (this.epoch == o.epoch) {
            return (int) (this.index - o.index);
        }
        return this.epoch - o.epoch;
    }

    public int getEpoch() {
        return epoch;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Offset offset = (Offset) o;
        return epoch == offset.epoch && index == offset.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, index);
    }

    @Override
    public String toString() {
        return "offset{" +
                "epoch=" + epoch +
                ", index=" + index +
                '}';
    }
}
