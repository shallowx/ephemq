package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.function.Function;

public class ChunkRecordHandler extends AbstractHandler<ChunkRecordSynchronization, ChunkRecordHandler> {
    static final ChunkRecordHandler INSTANCE = new ChunkRecordHandler();
    private final List<ChunkRecordSynchronization> synchronizations = new ArrayList<>();
    public ChunkRecordHandler() {
        super(null);
    }
    public ChunkRecordHandler(EventExecutor executor) {
        super(executor);
    }

    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<ChunkRecordHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    @Override
    Function<EventExecutor, ChunkRecordHandler> apply() {
        return ChunkRecordHandler::new;
    }

    public List<ChunkRecordSynchronization> getSynchronizations() {
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

