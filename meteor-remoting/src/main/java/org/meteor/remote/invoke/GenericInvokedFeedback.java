package org.meteor.remote.invoke;

import io.netty.util.ReferenceCounted;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.ReferenceCountUtil.release;
import static org.meteor.common.util.ObjectUtil.checkNotNull;

/**
 * A generic implementation of the InvokedFeedback interface that handles the invocation of a callback
 * upon the completion of an operation. It ensures that the callback is invoked only once and can handle
 * both success and failure scenarios.
 *
 * @param <V> the type of the result passed to the callback upon successful completion
 */
@SuppressWarnings("ALL")
public final class GenericInvokedFeedback<V> implements InvokedFeedback<V> {
    private static final AtomicIntegerFieldUpdater<GenericInvokedFeedback> UPDATER = AtomicIntegerFieldUpdater.newUpdater(GenericInvokedFeedback.class, "completed");
    /**
     * A callable object that represents the callback to be invoked upon the completion of an operation.
     * It is expected to handle both success and failure scenarios of the operation.
     */
    private final Callable<V> callback;
    /**
     * The initial expected state of the operation's completion status.
     * This value is used in conjunction with an AtomicIntegerFieldUpdater to ensure
     * the callback is invoked only once upon the completion of an operation.
     */
    private static final int EXPECT = 0;
    /**
     * Represents the state update value indicating that the operation has been updated/completed.
     * Used in comparison to atomic field updates for managing the state of completion.
     */
    private static final int UPDATE = 1;
    /**
     * Represents the completion status of the operation.
     * <p>
     * This variable is tracked using an {@link AtomicIntegerFieldUpdater} to
     * ensure atomic updates. It can be in one of two states:
     * {@code EXPECT} (0) indicating the operation is not completed, or
     * {@code UPDATE} (1) indicating the operation is completed.
     * The completion can be triggered by either a successful operation or a failure.
     */
    private volatile int completed;


    /**
     * Constructs a new GenericInvokedFeedback instance with the specified callback.
     *
     * @param callback the callback to be invoked upon the completion of an operation
     */
    public GenericInvokedFeedback(Callable<V> callback) {
        this.callback = callback;
    }

    /**
     * Checks if the operation has been completed.
     *
     * @return true if the operation is completed, false otherwise
     */
    @Override
    public boolean isCompleted() {
        return completed != EXPECT;
    }

    /**
     * Marks the operation as completed successfully and triggers the associated callback, if not marked before.
     *
     * @param v the result of the operation if it completed successfully
     * @return {@code true} if the operation was successfully marked as completed and the callback was triggered;
     *         {@code false} if the operation was already marked as completed
     */
    @Override
    public boolean success(V v) {
        try {
            if (UPDATER.compareAndSet(this, EXPECT, UPDATE)) {
                onCompleted(v, null);
                return true;
            }
            return false;
        } finally {
            if (v instanceof ReferenceCounted buf) {
                release(buf);
            }
        }
    }

    /**
     * Handles the failure scenario by invoking the callback with the provided Throwable cause.
     *
     * @param cause the Throwable cause that describes the failure; must not be null
     * @return true if the failure callback was successfully invoked, false otherwise
     */
    @Override
    public boolean failure(Throwable cause) {
        checkNotNull(cause, "Throwable cause must be not null");
        if (UPDATER.compareAndSet(this, EXPECT, UPDATE)) {
            onCompleted(null, cause);
            return true;
        }
        return false;
    }

    /**
     * Invokes the callback's onCompleted method with the provided result or cause.
     *
     * @param v the result of the operation, can be null if the operation failed
     * @param cause the throwable cause of the failure, can be null if the operation succeeded
     */
    private void onCompleted(V v, Throwable cause) {
        if (null != callback) {
            callback.onCompleted(v, cause);
        }
    }
}
