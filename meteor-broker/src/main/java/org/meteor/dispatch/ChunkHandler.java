package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.function.Function;

final class ChunkHandler extends AbstractHandler<ChunkSynchronization, ChunkHandler> {
    static final ChunkHandler INSTANCE = new ChunkHandler();
    private final List<ChunkSynchronization> synchronizations = new ArrayList<>();

    public ChunkHandler() {
        super(null);
    }

    public ChunkHandler(EventExecutor executor) {
        super(executor);
    }

    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<ChunkHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    @Override
    Function<EventExecutor, ChunkHandler> apply() {
        return ChunkHandler::new;
    }

    public List<ChunkSynchronization> getSynchronizations() {
        return synchronizations;
    }

    @Override
    public String toString() {
        return "ChunkRecordHandler{" +
                "synchronizations=" + synchronizations +
                ", subscriptionChannels=" + subscriptionChannels +
                ", triggered=" + triggered +
                ", dispatchExecutor=" + dispatchExecutor +
                ", followOffset=" + followOffset +
                ", followCursor=" + followCursor +
                '}';
    }
}

