package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;

final class DefaultHandler extends AbstractHandler<DefaultSynchronization, DefaultHandler> {
    static final DefaultHandler INSTANCE = new DefaultHandler();
    private final Int2ObjectMap<Set<DefaultSynchronization>> subscriptionMarkers = new Int2ObjectOpenHashMap<>();

    public DefaultHandler() {
        super(null);
    }

    public DefaultHandler(EventExecutor executor) {
        super(executor);
    }

    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<DefaultHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    @Override
    Function<EventExecutor, DefaultHandler> apply() {
        return DefaultHandler::new;
    }

    public Int2ObjectMap<Set<DefaultSynchronization>> getSubscriptionMarkers() {
        return subscriptionMarkers;
    }

    @Override
    public String toString() {
        return "(" +
                "subscriptionMarkers=" + subscriptionMarkers +
                ", subscriptionChannels=" + subscriptionChannels +
                ", triggered=" + triggered +
                ", dispatchExecutor=" + dispatchExecutor +
                ", followOffset=" + followOffset +
                ", followCursor=" + followCursor +
                ')';
    }
}
