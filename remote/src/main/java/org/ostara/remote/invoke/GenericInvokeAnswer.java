package org.ostara.remote.invoke;

import io.netty.util.ReferenceCounted;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.ReferenceCountUtil.release;
import static org.ostara.common.util.ObjectUtils.checkNotNull;

public final class GenericInvokeAnswer<V> implements InvokeAnswer<V> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<GenericInvokeAnswer> UPDATER = AtomicIntegerFieldUpdater.newUpdater(GenericInvokeAnswer.class, "completed");

    private static final int EXPECT = 0;
    private static final int UPDATE = 1;
    private final Callback<V> callback;
    private volatile int completed;

    public GenericInvokeAnswer() {
        this(null);
    }

    public GenericInvokeAnswer(Callback<V> callback) {
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
            callback.operationCompleted(v, cause);
        }
    }
}
