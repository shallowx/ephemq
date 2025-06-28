package org.meteor.dispatch;

import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

import javax.annotation.concurrent.Immutable;

/**
 * Represents a task that tracks the pursuit of a specific subscription in a ledger.
 * <p>
 * This immutable class encapsulates the necessary information required to continue
 * processing from a specific point in time within the ledger.
 *
 * @param <T> The type of the subscription.
 */
@Immutable
final class PursueTask<T> {
    /**
     * Represents the subscription being pursued in the ledger.
     * This subscription is the primary identifier used to resume
     * tasks from a specific point in the ledger.
     *
     */
    private final T subscription;
    /**
     * The cursor associated with a specific point in time within the ledger for the task.
     * It enables the task to continue processing from this specific point.
     */
    private final LedgerCursor cursor;
    /**
     * The timestamp indicating when the pursuit task was instantiated.
     * This value captures the system's current time in milliseconds at
     * the point of the task's creation.
     */
    private final long pursueTimeMillis = System.currentTimeMillis();
    /**
     * The {@code pursueOffset} represents the specific point in the ledger from which
     * the task continues its pursuit. It is used to track the position within the ledger
     * when processing the subscription.
     */
    private Offset pursueOffset;

    /**
     * Constructs a new PursueTask which tracks the pursuit of a specific subscription in a ledger.
     *
     * @param subscription the subscription data used for this task.
     * @param cursor       the cursor that provides the current position in the ledger.
     * @param pursueOffset the offset indicating where to continue the pursuit in the ledger.
     */
    public PursueTask(T subscription, LedgerCursor cursor, Offset pursueOffset) {
        this.subscription = subscription;
        this.cursor = cursor;
        this.pursueOffset = pursueOffset;
    }

    /**
     * Retrieves the subscription associated with this task.
     *
     * @return the subscription of type T.
     */
    public T getSubscription() {
        return subscription;
    }

    /**
     * Retrieves the current ledger cursor associated with the PursueTask.
     *
     * @return the ledger cursor used to track the position within the ledger.
     */
    public LedgerCursor getCursor() {
        return cursor;
    }

    /**
     * Retrieves the timestamp in milliseconds at which the pursuit task was created.
     *
     * @return the timestamp in milliseconds representing when the pursue task was created.
     */
    public long getPursueTimeMillis() {
        return pursueTimeMillis;
    }

    /**
     * Retrieves the offset from which the pursuing operation will continue.
     *
     * @return the pursue offset
     */
    public Offset getPursueOffset() {
        return pursueOffset;
    }

    /**
     * Sets the pursue offset for the task.
     *
     * @param pursueOffset the new offset to be set.
     */
    public void setPursueOffset(Offset pursueOffset) {
        this.pursueOffset = pursueOffset;
    }

    /**
     * Returns a string representation of the PursueTask object.
     *
     * @return a string representation of the PursueTask object in the format:
     *         (subscription=subscription, cursor=cursor, pursueTimeMillis=pursueTimeMillis, pursueOffset=pursueOffset)
     */
    @Override
    public String toString() {
        return "PursueTask (subscription=%s, cursor=%s, pursueTimeMillis=%d, pursueOffset=%s)".formatted(subscription, cursor, pursueTimeMillis, pursueOffset);
    }
}
