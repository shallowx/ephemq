package org.meteor.dispatch;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

import javax.annotation.concurrent.Immutable;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Immutable
public class Handler {
    private final String id = UUID.randomUUID().toString();
    private final ConcurrentMap<Channel, Subscription> channelSubscriptionMap = new ConcurrentHashMap<>();
    private final Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = new Int2ObjectOpenHashMap<>();
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    private final EventExecutor dispatchExecutor;
    private volatile Offset followOffset;
    private volatile LedgerCursor followCursor;
    public Handler(EventExecutor executor) {
        this.dispatchExecutor = executor;
    }

    public static Handler newHandler(WeakHashMap<Handler, Integer> allocateHandlers, EventExecutor[] executors ) {
        synchronized (allocateHandlers) {
            int[] countArray = new int[executors.length];
            allocateHandlers.values().forEach(i -> countArray[i]++);
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

            Handler result = new Handler(executors[index]);
            allocateHandlers.put(result, index);
            return result;
        }
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

    public ConcurrentMap<Channel, Subscription> getChannelSubscriptionMap() {
        return channelSubscriptionMap;
    }

    public Int2ObjectMap<Set<Subscription>> getMarkerSubscriptionMap() {
        return markerSubscriptionMap;
    }

    public EventExecutor getDispatchExecutor() {
        return dispatchExecutor;
    }

    @Override
    public String toString() {
        return "Handler{" +
                "id='" + id + '\'' +
                ", channelSubscriptionMap=" + channelSubscriptionMap +
                ", markerSubscriptionMap=" + markerSubscriptionMap +
                ", triggered=" + triggered +
                ", dispatchExecutor=" + dispatchExecutor +
                ", followOffset=" + followOffset +
                ", followCursor=" + followCursor +
                '}';
    }
}
