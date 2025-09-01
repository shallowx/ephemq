package org.ephemq.remote.invoke;

/**
 * The InvokedFeedback interface represents a feedback mechanism for operations
 * that can be completed successfully or failed with a specific cause.
 *
 * @param <V> the type of result the feedback mechanism handles upon successful completion
 */
public interface InvokedFeedback<V> {
    /**
     * Checks if the operation associated with this feedback has been completed.
     *
     * @return true if the operation is completed, false otherwise
     */
    boolean isCompleted();

    /**
     * Indicates the success of an operation by providing a relevant result.
     *
     * @param v the result of the operation upon successful completion
     * @return true if the operation succeeded, false otherwise
     */
    boolean success(V v);

    /**
     * Marks the operation as failed with the specified cause.
     *
     * @param cause the {@link Throwable} that caused the failure
     * @return true if the failure was successfully recorded, false otherwise
     */
    boolean failure(Throwable cause);
}
