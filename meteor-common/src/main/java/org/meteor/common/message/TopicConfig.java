package org.meteor.common.message;

/**
 * The TopicConfig class holds configuration settings for a topic.
 * It includes settings such as segment rolling size, segment retention count,
 * and segment retention time (in milliseconds).
 */
public class TopicConfig {
    /**
     * Specifies the maximum size (in bytes) a segment file can grow to before a new segment is rolled.
     * This setting is essential for managing the size and number of segment files for a topic efficiently.
     */
    private int segmentRollingSize;
    /**
     * The number of segments to retain for a topic.
     * This variable determines how many old segments are preserved before deletion.
     */
    private int segmentRetainCount;
    /**
     *
     */
    private int segmentRetainMs;
    /**
     * Indicates whether resources should be allocated for the topic.
     */
    private boolean allocate;

    /**
     * Default constructor for TopicConfig.
     */
    public TopicConfig() {
    }

    /**
     * Constructor for the TopicConfig class.
     *
     * @param segmentRollingSize The size at which the log segment rolls over.
     * @param segmentRetainCount The number of segments to retain.
     * @param segmentRetainMs    The duration (in milliseconds) to retain a segment.
     * @param allocate           A boolean flag indicating if the segment should be allocated.
     */
    public TopicConfig(int segmentRollingSize, int segmentRetainCount, int segmentRetainMs, boolean allocate) {
        this.segmentRollingSize = segmentRollingSize;
        this.segmentRetainCount = segmentRetainCount;
        this.segmentRetainMs = segmentRetainMs;
        this.allocate = allocate;
    }

    /**
     * Retrieves the maximum size before a segment is rolled over for this topic configuration.
     *
     * @return the segment rolling size in bytes.
     */
    public int getSegmentRollingSize() {
        return segmentRollingSize;
    }

    /**
     * Sets the segment rolling size for the topic.
     *
     * @param segmentRollingSize The new size (in bytes) at which the segment rolls over.
     */
    public void setSegmentRollingSize(int segmentRollingSize) {
        this.segmentRollingSize = segmentRollingSize;
    }

    /**
     * Returns the segment retain count for the topic configuration.
     *
     * @return The segment retain count.
     */
    public int getSegmentRetainCount() {
        return segmentRetainCount;
    }

    /**
     * Sets the number of segments to retain.
     *
     * @param segmentRetainCount the number of segments to retain
     */
    public void setSegmentRetainCount(int segmentRetainCount) {
        this.segmentRetainCount = segmentRetainCount;
    }

    /**
     * Retrieves the segment retention time in milliseconds.
     *
     * @return the segment retention time in milliseconds.
     */
    public int getSegmentRetainMs() {
        return segmentRetainMs;
    }

    /**
     * Sets the segment retention time in milliseconds.
     *
     * @param segmentRetainMs the segment retention time in milliseconds
     */
    public void setSegmentRetainMs(int segmentRetainMs) {
        this.segmentRetainMs = segmentRetainMs;
    }

    /**
     * Checks if the topic is marked for allocation.
     *
     * @return {@code true} if the topic is allocated, {@code false} otherwise.
     */
    public boolean isAllocate() {
        return allocate;
    }

    /**
     * Sets whether allocation is enabled for the topic.
     *
     * @param allocate the allocation status to set
     */
    public void setAllocate(boolean allocate) {
        this.allocate = allocate;
    }

    /**
     * Returns a string representation of the TopicConfig object, which includes
     * the values of segmentRollingSize, segmentRetainCount, segmentRetainMs, and allocate properties.
     *
     * @return a string representation of the TopicConfig object
     */
    @Override
    public String toString() {
        return STR."(segment_rolling_size=\{segmentRollingSize}, segment_retain_count=\{segmentRetainCount}, segment_retainMs=\{segmentRetainMs}, allocate=\{allocate})";
    }
}
