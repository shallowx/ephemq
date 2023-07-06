package org.ostara.common;

public class TopicConfig {
    private int segmentRollingSize;
    private int segmentRetainCount;
    private int segmentRetainMs;

    public TopicConfig() {
    }

    public TopicConfig(int segmentRollingSize, int segmentRetainCount, int segmentRetainMs) {
        this.segmentRollingSize = segmentRollingSize;
        this.segmentRetainCount = segmentRetainCount;
        this.segmentRetainMs = segmentRetainMs;
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

    @Override
    public String toString() {
        return "topic_config {" +
                "segmentRollingSize=" + segmentRollingSize +
                ", segmentRetainCount=" + segmentRetainCount +
                ", segmentRetainMs=" + segmentRetainMs +
                '}';
    }
}
