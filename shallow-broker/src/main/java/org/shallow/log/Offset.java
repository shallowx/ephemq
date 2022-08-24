package org.shallow.log;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class Offset implements Comparable<Offset> {
    private final int epoch;
    private final long index;

    public Offset(int epoch, long index) {
        this.epoch = epoch;
        this.index = index;
    }

    public int epoch() {
        return epoch;
    }

    public long index() {
        return index;
    }

    public boolean after(Offset o) {
        return compareTo(o) > 0;
    }

    public boolean before(Offset o) {
        return compareTo(o) < 0;
    }

    @Override
    public int compareTo(Offset o) {
        if (this.epoch == o.epoch) {
            return (int) (this.index - o.index);
        }

        return this.epoch - o.epoch;
    }

    @Override
    public String toString() {
        return "Offset{" +
                "epoch=" + epoch +
                ", index=" + index +
                '}';
    }
}
