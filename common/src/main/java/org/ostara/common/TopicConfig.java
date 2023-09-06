package org.ostara.common;

public class TopicConfig {
    private int segmentRollingSize;
    private int segmentRetainCount;
    private int segmentRetainMs;
    private boolean allocate;

    public TopicConfig() {
    }

    public TopicConfig(int segmentRollingSize, int segmentRetainCount, int segmentRetainMs, boolean allocate) {
        this.segmentRollingSize = segmentRollingSize;
        this.segmentRetainCount = segmentRetainCount;
        this.segmentRetainMs = segmentRetainMs;
        this.allocate = allocate;
    }

    public int getSegmentRollingSize() {
        return segmentRollingSize;
    }

    public void setSegmentRollingSize(int segmentRollingSize) {
        this.segmentRollingSize = segmentRollingSize;
    }

    public int getSegmentRetainCount() {
        return segmentRetainCount;
    }

    public void setSegmentRetainCount(int segmentRetainCount) {
        this.segmentRetainCount = segmentRetainCount;
    }

    public int getSegmentRetainMs() {
        return segmentRetainMs;
    }

    public void setSegmentRetainMs(int segmentRetainMs) {
        this.segmentRetainMs = segmentRetainMs;
    }

    public boolean isAllocate() {
        return allocate;
    }

    public void setAllocate(boolean allocate) {
        this.allocate = allocate;
    }

    @Override
    public String toString() {
        return "TopicConfig{" +
                "segmentRollingSize=" + segmentRollingSize +
                ", segmentRetainCount=" + segmentRetainCount +
                ", segmentRetainMs=" + segmentRetainMs +
                ", allocate=" + allocate +
                '}';
    }
}
