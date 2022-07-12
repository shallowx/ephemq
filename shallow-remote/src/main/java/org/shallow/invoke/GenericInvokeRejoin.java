package org.shallow.invoke;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import static io.netty.util.ReferenceCountUtil.release;
import static org.shallow.ObjectUtil.checkNotNull;
import static org.shallow.ObjectUtil.isNotNull;

public class GenericInvokeRejoin<V> implements InvokeRejoin<V> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<GenericInvokeRejoin> UPDATER = AtomicIntegerFieldUpdater.newUpdater(GenericInvokeRejoin.class, "completed");

    private static final int EXPECT = 0;
    private static final int UPDATE = 1;

    private volatile int completed;
    private final Invoker<V> invoker;

    public GenericInvokeRejoin() {
        this(null);
    }

    public GenericInvokeRejoin(Invoker<V> invoker) {
        this.invoker = invoker;
    }

    @Override
    public boolean isCompleted() {
        return completed == UPDATE;
    }

    @Override
    public boolean success(V v) {
        try {
            if (UPDATER.compareAndSet(this, EXPECT, UPDATE)) {
                onCompleted(v, null);
                return true;
            }
            return  false;
        } finally {
          release(v);
        }
    }

    @Override
    public boolean failure(Throwable cause) {
        checkNotNull(cause, "cause must be not null");
        if (UPDATER.compareAndSet(this, EXPECT , UPDATE)) {
            onCompleted(null, cause);
            return true;
        }
        return false;
    }

    private void onCompleted(V v, Throwable cause) {
       if (isNotNull(invoker)) {
           invoker.onCompleted(v, cause);
       }
    }
}
