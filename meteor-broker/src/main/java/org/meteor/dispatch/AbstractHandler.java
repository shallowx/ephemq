package org.meteor.dispatch;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

abstract class AbstractHandler<E, T> {
    protected final ConcurrentMap<Channel, E> subscriptionChannels = new ConcurrentHashMap<>();
    protected final AtomicBoolean triggered = new AtomicBoolean(false);
    protected final EventExecutor dispatchExecutor;
    protected volatile Offset followOffset;
    protected volatile LedgerCursor followCursor;

    abstract int[] getCounts(EventExecutor[] executors, WeakHashMap<T, Integer> handlers);

    abstract Function<EventExecutor, T> apply();

    protected T newHandler(WeakHashMap<T, Integer> handlers, EventExecutor[] executors) {
        return newHandler(executors, handlers, apply());
    }

    private T newHandler(EventExecutor[] executors, WeakHashMap<T , Integer> handlers, Function<EventExecutor, T> f) {
        int[] countArray = getCounts(executors, handlers);
        int index = index(countArray);
        T result = f.apply(executors[index]);
        handlers.put(result, index);
        return result;
    }

    private int index(int[] countArray) {
        int index = 0;
        if (countArray[index] > 0) {
            for (int i = 1; i < countArray.length; i++) {
                int v = countArray[i];
                if (v == 0) {
                    index = i;
                    break;
                }

                if (v < countArray[i]) {
                    index = i;
                }
            }
        }
        return index;
    }

    public AbstractHandler(EventExecutor executor) {
        this.dispatchExecutor = executor;
    }

    public void setFollowOffset(Offset followOffset) {
        this.followOffset = followOffset;
    }

    public void setFollowCursor(LedgerCursor followCursor) {
        this.followCursor = followCursor;
    }

    public AtomicBoolean getTriggered() {
        return triggered;
    }

    public Offset getFollowOffset() {
        return followOffset;
    }

    public LedgerCursor getFollowCursor() {
        return followCursor;
    }

    public ConcurrentMap<Channel, E> getSubscriptionChannels() {
        return subscriptionChannels;
    }

    public EventExecutor getDispatchExecutor() {
        return dispatchExecutor;
    }
}
