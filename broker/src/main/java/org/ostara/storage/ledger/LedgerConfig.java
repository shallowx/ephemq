package org.ostara.storage.ledger;

public class LedgerConfig {
    private int segmentRetainCounts = 3;
    private int segmentRetainMs = 30000;
    private int segmentBufferCapacity = 4194304; // 4 * 1024 * 1024
    private boolean allocate;

    public int segmentRetainCounts() {
        return segmentRetainCounts;
    }

    public LedgerConfig segmentRetainCounts(int segmentRetainCounts) {
        this.segmentRetainCounts = segmentRetainCounts;
        return this;
    }

    public int segmentRetainMs() {
        return segmentRetainMs;
    }

    public LedgerConfig segmentRetainMs(int segmentRetainMs) {
        this.segmentRetainMs = segmentRetainMs;
        return this;
    }

    public int segmentBufferCapacity() {
        return segmentBufferCapacity;
    }

    public boolean isAllocate() {
        return allocate;
    }

    public LedgerConfig segmentBufferCapacity(int segmentBufferCapacity) {
        this.segmentBufferCapacity = segmentBufferCapacity;
        return this;
    }

    public LedgerConfig allocate(boolean allocate) {
        this.allocate = allocate;
        return this;
    }

}
