package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.function.Function;

public class RecordChunkHandler extends AbstractHandler<RecordChunkSynchronization, RecordChunkHandler> {
    static final RecordChunkHandler INSTANCE = new RecordChunkHandler();
    private final List<RecordChunkSynchronization> synchronizations = new ArrayList<>();

    public RecordChunkHandler() {
        super(null);
    }

    public RecordChunkHandler(EventExecutor executor) {
        super(executor);
    }

    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<RecordChunkHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    @Override
    Function<EventExecutor, RecordChunkHandler> apply() {
        return RecordChunkHandler::new;
    }

    public List<RecordChunkSynchronization> getSynchronizations() {
        return synchronizations;
    }

    @Override
    public String toString() {
        return "RecordChunkHandler{" +
                "synchronizations=" + synchronizations +
                ", id='" + id + '\'' +
                ", channelSubscriptionMap=" + channelSubscriptionMap +
                ", triggered=" + triggered +
                ", dispatchExecutor=" + dispatchExecutor +
                ", followOffset=" + followOffset +
                ", followCursor=" + followCursor +
                '}';
    }
}

