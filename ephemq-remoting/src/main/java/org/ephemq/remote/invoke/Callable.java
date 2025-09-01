package org.ephemq.remote.invoke;

/**
 * A functional interface representing a callback to be invoked upon the completion of an operation.
 *
 * @param <V> the type of the result passed to the callback upon successful completion
 */
@FunctionalInterface
public interface Callable<V> {
    /**
     * This method is called when an operation is completed, either successfully or with a failure.
     *
     * @param v     the result of the operation if it completed successfully; null if there was a failure
     * @param cause the throwable that caused the failure; null if the operation completed successfully
     */
    void onCompleted(V v, Throwable cause);
}
