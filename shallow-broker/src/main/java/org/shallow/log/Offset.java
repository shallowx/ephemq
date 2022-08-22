package org.shallow.log;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class Offset {
    private int epoch;
    private long index;

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

    @Override
    public String toString() {
        return "Offset{" +
                "epoch=" + epoch +
                ", index=" + index +
                '}';
    }
}
