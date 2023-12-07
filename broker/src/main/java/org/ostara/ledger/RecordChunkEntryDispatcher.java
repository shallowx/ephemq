package org.ostara.ledger;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ostara.common.Offset;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

public class RecordChunkEntryDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RecordEntryDispatcher.class);

    private final int ledger;
    private final String topic;
    private final LedgerStorage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int alignLimit;
    private final long pursueTimeoutMs;
    private final int loadLimit;
    private final int pursueBytesLimit;
    private final int bytesLimit;
    private final IntConsumer counter;
    private final EventExecutor[] executors;
    private final List<Handler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final WeakHashMap<Handler, Integer> allocateHandlers = new WeakHashMap<>();
    private final ConcurrentMap<Channel, Handler> channelHandlerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    public RecordChunkEntryDispatcher(int ledger, String topic, LedgerStorage storage, CoreConfig config, EventExecutorGroup executorGroup,IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getChunkDispatchEntryFollowLimit();
        this.pursueLimit = config.getChunkDispatchEntryPursueLimit();
        this.alignLimit = config.getChunkDispatchEntryAlignLimit();
        this.pursueTimeoutMs = config.getChunkDispatchEntryPursueTimeoutMs();
        this.loadLimit = config.getChunkDispatchEntryLoadLimit();
        this.bytesLimit = config.getChunkDispatchEntryBytesLimit();
        this.pursueBytesLimit = bytesLimit + config.getChunkDispatchEntryPursueLimit();
        this.counter = dispatchCounter;

        List<EventExecutor> eventExecutors = new ArrayList<>();
        executorGroup.forEach(eventExecutors::add);
        Collections.shuffle(eventExecutors);
        this.executors = eventExecutors.toArray(new EventExecutor[0]);
    }

    public int channelCount() {
        return channelHandlerMap.size();
    }

    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    public void detachAll() {
        for (Channel channel : channelHandlerMap.keySet()) {
            detach(channel, ImmediateEventExecutor.INSTANCE.newPromise());
        }
    }

    public void detach(Channel channel, Promise<Void> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doDetach(channel, promise);
            } else {
                executor.execute(() -> doDetach(channel, promise));
            }
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    private void doDetach(Channel channel, Promise<Void> promise) {
        // TODO
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }
        for (Handler handler : dispatchHandlers) {
            if (handler.followCursor != null) {
                touchDispatch(handler);
            }
        }
    }

    private void touchDispatch(Handler handler) {
        if (handler.triggered.compareAndSet(false,true)) {
            try {
                handler.dispatchExecutor.execute(() -> doDispatch(handler));
            } catch (Exception e) {
                logger.error("Chunk submit dispatch failed", e);
            }
        }
    }

    private void doDispatch(Handler handler) {
        // TODO
    }

    public void attach(Channel channel, Offset initOffset, Promise<Void> promise) {
        // TODO
    }

    private class Handler {
        private final String id = UUID.randomUUID().toString();
        private final ConcurrentMap<Channel, Synchronization> channelSynchronizationMap = new ConcurrentHashMap<>();
        private final List<Synchronization> Synchronizations = new ArrayList<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);
        private final EventExecutor dispatchExecutor;
        private volatile Offset followOffset;
        private volatile LedgerCursor followCursor;

        private Handler(EventExecutor dispatchExecutor) {
            this.dispatchExecutor = dispatchExecutor;
        }

        @Override
        public String toString() {
            return storage +
                    "handler=" + id +
                    "followOffset=" + followOffset +
                    "allchannels=" + channelSynchronizationMap.size();
        }
    }

    private class Synchronization {
        private final Channel channel;
        private final Handler handler;
        private Offset dispatchOffset;
        private boolean followed = false;

        public Synchronization(Channel channel, Handler handler) {
            this.channel = channel;
            this.handler = handler;
        }

        @Override
        public String toString() {
            return "Synchronization{" +
                    "channel=" + channel +
                    ", handler=" + handler +
                    ", dispatchOffset=" + dispatchOffset +
                    '}';
        }
    }

    private class PursueTask {
        private final Synchronization synchronization;
        private final LedgerCursor pursueCursor;
        private final long pursueTime = System.currentTimeMillis();
        private Offset pursueOffset;

        public PursueTask(Synchronization synchronization, LedgerCursor pursueCursor, Offset pursueOffset) {
            this.synchronization = synchronization;
            this.pursueCursor = pursueCursor;
            this.pursueOffset = pursueOffset;
        }

        @Override
        public String toString() {
            return "PursueTask{" +
                    "synchronization=" + synchronization +
                    ", pursueCursor=" + pursueCursor +
                    ", pursueTime=" + pursueTime +
                    ", pursueOffset=" + pursueOffset +
                    '}';
        }
    }
}
