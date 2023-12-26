package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;

public class RecordHandler extends AbstractHandler<RecordSynchronization, RecordHandler> {
    static final RecordHandler INSTANCE = new RecordHandler();
    protected Int2ObjectMap<Set<RecordSynchronization>> markerSubscriptionMap = new Int2ObjectOpenHashMap<>();

    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<RecordHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    public RecordHandler() {
        super(null);
    }

    public RecordHandler(EventExecutor executor) {
        super(executor);
    }

    public Int2ObjectMap<Set<RecordSynchronization>> getMarkerSubscriptionMap() {
        return markerSubscriptionMap;
    }

    @Override
    Function<EventExecutor, RecordHandler> apply() {
        return RecordHandler::new;
    }

    @Override
    public String toString() {
        return "record_handler{" +
                "markerSubscriptionMap=" + markerSubscriptionMap +
                ", id='" + id + '\'' +
                ", channelSubscriptionMap=" + channelSubscriptionMap +
                ", triggered=" + triggered +
                ", dispatchExecutor=" + dispatchExecutor +
                ", followOffset=" + followOffset +
                ", followCursor=" + followCursor +
                '}';
    }
}
