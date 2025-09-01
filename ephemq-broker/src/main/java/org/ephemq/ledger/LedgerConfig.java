package org.ephemq.ledger;

/**
 * The {@code LedgerConfig} class represents the configuration settings for a ledger.
 * It allows specifying various parameters related to ledger segments such as
 * retention counts, retention time, buffer capacity, and allocation state.
 */
public class LedgerConfig {
    /**
     * The number of ledger segments to retain.
     * This variable defines the number of recent ledger segments
     * that should be kept in memory to allow for quick access and
     * to avoid excessive disk I/O operations. Keeping more segments
     * can be useful for recovering recent data quickly, but it may
     * increase memory usage.
     */
    private int segmentRetainCounts = 3;
    /**
     * The duration (in milliseconds) that a ledger segment should be retained.
     * This setting determines how long the segments are preserved before they
     * are eligible for deletion or other forms of data lifecycle management.
     */
    private int segmentRetainMs = 30000;
    /**
     * The capacity (in bytes) allocated for a buffer associated with
     * ledger segments. This determines the maximum size of the segment buffer.
     */
    private int segmentBufferCapacity = 4194304;
    /**
     * Indicates whether a ledger segment is currently allocated.
     * This flag is used to manage the state of ledger segments
     * in terms of their allocation status.
     */
    private boolean isAlloc;

    /**
     * Gets the number of segments to retain.
     *
     * @return the number of segments to retain.
     */
    public int segmentRetainCounts() {
        return segmentRetainCounts;
    }

    /**
     * Sets the number of segments to retain in the ledger configuration.
     *
     * @param segmentRetainCounts the number of segments to retain
     * @return the updated {@code LedgerConfig} instance
     */
    public LedgerConfig segmentRetainCounts(int segmentRetainCounts) {
        this.segmentRetainCounts = segmentRetainCounts;
        return this;
    }

    /**
     * Returns the retention time for ledger segments in milliseconds.
     *
     * @return the retention time for ledger segments in milliseconds.
     */
    public int segmentRetainMs() {
        return segmentRetainMs;
    }

    /**
     * Sets the segment retention time in milliseconds and returns the updated LedgerConfig object.
     *
     * @param segmentRetainMs the segment retention time in milliseconds.
     * @return the updated LedgerConfig object.
     */
    public LedgerConfig segmentRetainMs(int segmentRetainMs) {
        this.segmentRetainMs = segmentRetainMs;
        return this;
    }

    /**
     * Returns the capacity of the segment buffer.
     *
     * @return the capacity of the segment buffer in bytes.
     */
    public int segmentBufferCapacity() {
        return segmentBufferCapacity;
    }

    /**
     * Returns whether the allocation is enabled for the ledger configuration.
     *
     * @return {@code true} if allocation is enabled, {@code false} otherwise
     */
    public boolean isAlloc() {
        return isAlloc;
    }

    /**
     * Sets the buffer capacity for ledger segments.
     *
     * @param segmentBufferCapacity the desired buffer capacity for each segment
     * @return the updated LedgerConfig instance
     */
    public LedgerConfig segmentBufferCapacity(int segmentBufferCapacity) {
        this.segmentBufferCapacity = segmentBufferCapacity;
        return this;
    }

    /**
     * Sets the allocation state for the LedgerConfig and returns
     * the current instance of LedgerConfig.
     *
     * @param alloc the boolean value to set the allocation state
     * @return the current instance of LedgerConfig with updated allocation state
     */
    public LedgerConfig alloc(boolean alloc) {
        this.isAlloc = alloc;
        return this;
    }

}
