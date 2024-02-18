package org.meteor.remote.invoke;

import io.netty.util.ReferenceCounted;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.ReferenceCountUtil.release;
import static org.meteor.common.util.ObjectUtil.checkNotNull;

public final class GenericInvokedFeedback<V> implements InvokedFeedback<V> {
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<GenericInvokedFeedback> UPDATER = AtomicIntegerFieldUpdater.newUpdater(GenericInvokedFeedback.class, "completed");
    private static final int EXPECT = 0;
    private static final int UPDATE = 1;
    private final Callable<V> callback;
    private volatile int completed;

    public GenericInvokedFeedback() {
        this(null);
    }

    public GenericInvokedFeedback(Callable<V> callback) {
        this.callback = callback;
    }

    @Override
    public boolean isCompleted() {
        return completed != EXPECT;
    }

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

    @Override
    public boolean failure(Throwable cause) {
        checkNotNull(cause, "Throwable cause must be not null");
        if (UPDATER.compareAndSet(this, EXPECT, UPDATE)) {
            onCompleted(null, cause);
            return true;
        }
        return false;
    }

    private void onCompleted(V v, Throwable cause) {
        if (null != callback) {
            callback.onCompleted(v, cause);
        }
    }
}
