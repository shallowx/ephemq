package org.meteor.ledger;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntConsumer;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.ServerConfig;
import org.meteor.coordinator.Coordinator;
import org.meteor.coordinator.TopicCoordinator;
import org.meteor.dispatch.ChunkRecordDispatcher;
import org.meteor.dispatch.RecordDispatcher;
import org.meteor.listener.LogListener;
import org.meteor.metrics.config.MetricsConstants;
import org.meteor.remote.invoke.Callable;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.invoke.RemoteException;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Log {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Log.class);

    protected final TopicPartition topicPartition;
    protected final int ledger;
    protected final String topic;
    protected final LedgerStorage storage;
    protected final EventExecutor storageExecutor;
    protected final EventExecutor commandExecutor;
    protected final RecordDispatcher entryDispatcher;
    protected final List<LogListener> listeners;
    protected final Coordinator coordinator;
    protected final Meter segmentCount;
    protected final Meter segmentBytes;
    protected final int forwardTimeout;
    protected final AtomicReference<LogState> state = new AtomicReference<>(null);
    protected ChunkRecordDispatcher chunkEntryDispatcher;
    protected Migration migration;
    protected ClientChannel syncChannel;
    protected Promise<SyncResponse> syncPromise;
    protected Promise<CancelSyncResponse> cancelSyncPromise;

    public Log(ServerConfig config, TopicPartition topicPartition, int ledger, int epoch, Coordinator coordinator, TopicConfig topicConfig) {
        this.topicPartition = topicPartition;
        this.ledger = ledger;
        this.topic = topicPartition.topic();
        this.forwardTimeout = config.getRecordDispatchConfig().getDispatchEntryFollowLimit();
        this.commandExecutor = coordinator.getCommandHandleEventExecutorGroup().next();
        LedgerConfig ledgerConfig;
        if (topicConfig != null) {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(topicConfig.getSegmentRetainCount())
                    .segmentBufferCapacity(topicConfig.getSegmentRollingSize())
                    .segmentRetainMs(topicConfig.getSegmentRetainMs())
                    .alloc(topicConfig.isAllocate());
        } else {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(config.getSegmentConfig().getSegmentRetainLimit())
                    .segmentBufferCapacity(config.getSegmentConfig().getSegmentRollingSize())
                    .segmentRetainMs(config.getSegmentConfig().getSegmentRetainTimeMilliseconds())
                    .alloc(false);
        }
        this.storageExecutor = coordinator.getMessageStorageEventExecutorGroup().next();
        this.storage = new LedgerStorage(ledger, topicPartition.topic(), epoch, ledgerConfig, storageExecutor, new InnerTrigger());
        this.coordinator = coordinator;
        this.listeners = coordinator.getLogCoordinator().getLogListeners();
        Tags tags = Tags.of(MetricsConstants.TOPIC_TAG, topicPartition.topic())
                .and(MetricsConstants.PARTITION_TAG, String.valueOf(topicPartition.partition()))
                .and(MetricsConstants.BROKER_TAG, config.getCommonConfig().getServerId())
                .and(MetricsConstants.CLUSTER_TAG, config.getCommonConfig().getClusterName())
                .and(MetricsConstants.LEDGER_TAG, Integer.toString(ledger));
        this.segmentCount = Gauge.builder(MetricsConstants.LOG_SEGMENT_COUNT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentCount)
                .tags(tags).register(Metrics.globalRegistry);
        this.segmentBytes = Gauge.builder(MetricsConstants.LOG_SEGMENT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentBytes)
                .baseUnit("bytes")
                .tags(tags).register(Metrics.globalRegistry);
        this.entryDispatcher = new RecordDispatcher(ledger, topic, storage, config.getRecordDispatchConfig(), coordinator.getMessageDispatchEventExecutorGroup(), new InnerEntryDispatchCounter());
        this.chunkEntryDispatcher = new ChunkRecordDispatcher(ledger, topic, storage, config.getChunkRecordDispatchConfig(), coordinator.getMessageDispatchEventExecutorGroup(), new InnerEntryChunkDispatchCounter());
    }

    public ClientChannel getSyncChannel() {
        return syncChannel;
    }

    public int getLedger() {
        return ledger;
    }

    public int getEpoch() {
        return storage.currentOffset().getEpoch();
    }

    public void updateEpoch(int epoch) {
        storage.updateEpoch(epoch);
    }

    public void migrate(String dest, ClientChannel destChannel, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doMigrate(dest, destChannel, promise);
        } else {
            try {
                storageExecutor.submit(() -> doMigrate(dest, destChannel, promise));
            } catch (Exception e) {
                promise.tryFailure(e);
                logger.error(e.getMessage(), e);
            }
        }
    }

    public int getSubscriberCount() {
        return entryDispatcher.channelCount() + chunkEntryDispatcher.channelCount();
    }

    public Promise<SyncResponse> syncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs) {
        Promise<SyncResponse> result = storageExecutor.newPromise();
        if (storageExecutor.inEventLoop()) {
            doSyncFromTarget(clientChannel, offset, timeoutMs, result);
        } else {
            try {
                storageExecutor.submit(() -> doSyncFromTarget(clientChannel, offset, timeoutMs, result));
            } catch (Throwable t) {
                result.tryFailure(t);
                logger.error(t.getMessage(), t);
            }
        }
        return result;
    }

    protected void doSyncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs, Promise<SyncResponse> promise) {
        if (syncPromise != null && !syncPromise.isDone()) {
            syncPromise.addListener(f -> doSyncFromTarget(clientChannel, offset, timeoutMs, promise));
            return;
        }

        if (cancelSyncPromise != null && !cancelSyncPromise.isDone()) {
            cancelSyncPromise.addListener(f -> doSyncFromTarget(clientChannel, offset, timeoutMs, promise));
            return;
        }

        syncPromise = promise;
        promise.addListener(future -> syncPromise = null);
        LogState logState = state.get();
        if (!isAppendable(logState) && !isMigrating(logState)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION,
                    String.format("The log can't begin to sync data,the current state is %s", logState)));
            return;
        }

        if (!isMigrating(logState)) {
            state.set(LogState.SYNCHRONIZING);
        }

        Promise<SyncResponse> syncPromise = storageExecutor.newPromise();
        syncPromise.addListener((GenericFutureListener<Future<SyncResponse>>) f -> {
            if (f.isSuccess()) {
                syncChannel = clientChannel;
                promise.trySuccess(f.getNow());
            } else {
                LogState logState1 = state.get();
                if (logState1 != logState) {
                    state.set(logState);
                }
                promise.tryFailure(f.cause());
            }
        });

        try {
            Offset currentOffset = getCurrentOffset();
            Offset syncOffset;
            if (offset == null) {
                syncOffset = new Offset(-1, -1L);
            } else if (offset.after(currentOffset)) {
                syncOffset = offset;
            } else {
                syncOffset = currentOffset;
            }
            TopicCoordinator topicCoordinator = coordinator.getTopicCoordinator();
            topicCoordinator.getParticipantCoordinator().syncLeader(topicPartition, ledger, clientChannel, syncOffset.getEpoch(), syncOffset.getIndex(), timeoutMs, syncPromise);
        } catch (Exception e) {
            syncPromise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    public Promise<CancelSyncResponse> cancelSync(int timeoutMs) {
        Promise<CancelSyncResponse> promise = storageExecutor.newPromise();
        if (storageExecutor.inEventLoop()) {
            doCancelSync(timeoutMs, promise);
        } else {
            try {
                storageExecutor.submit(() -> doCancelSync(timeoutMs, promise));
            } catch (Exception e) {
                promise.tryFailure(e);
                logger.error(e.getMessage(), e);
            }
        }
        return promise;
    }

    private void doCancelSync(int timeoutMs, Promise<CancelSyncResponse> promise) {
        if (syncPromise != null && !syncPromise.isDone()) {
            syncPromise.addListener(future -> doCancelSync(timeoutMs, promise));
            return;
        }

        if (cancelSyncPromise != null && !cancelSyncPromise.isDone()) {
            cancelSyncPromise.addListener(future -> doCancelSync(timeoutMs, promise));
            return;
        }

        cancelSyncPromise = promise;
        promise.addListener(future -> cancelSyncPromise = null);
        LogState logState = state.get();
        if (!isSynchronizing(logState) && !isMigrating(logState)) {
            promise.trySuccess(null);
            return;
        }

        state.set(LogState.APPENDABLE);
        Promise<CancelSyncResponse> unSyncPromise = storageExecutor.newPromise();
        unSyncPromise.addListener(future -> {
            if (future.isSuccess()) {
                syncChannel = null;
                promise.trySuccess((CancelSyncResponse) future.getNow());
            } else {
                state.set(logState);
                promise.tryFailure(future.cause());
            }
        });
        if (syncChannel == null) {
            unSyncPromise.trySuccess(null);
            return;
        }
        if (!syncChannel.isActive()) {
            unSyncPromise.trySuccess(null);
            return;
        }
        try {
            TopicCoordinator topicCoordinator = coordinator.getTopicCoordinator();
            topicCoordinator.getParticipantCoordinator().unSyncLedger(topicPartition, ledger, syncChannel, timeoutMs, unSyncPromise);
        } catch (Exception e) {
            unSyncPromise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    private void doMigrate(String dest, ClientChannel destChannel, Promise<Void> promise) {
        LogState logState = state.get();
        if (!isAppendable(logState)) {
            return;
        }

        migration = new Migration(ledger, destChannel);
        state.set(LogState.MIGRATING);
        try {
            TopicCoordinator topicCoordinator = coordinator.getTopicCoordinator();
            Promise<SyncResponse> syncResponsePromise = syncFromTarget(destChannel, new Offset(0, 0L), 30000);
            syncResponsePromise.addListener(future -> {
                if (future.isSuccess()) {
                    commandExecutor.schedule(() -> {
                        try {
                            topicCoordinator.handoverPartition(dest, topicPartition);
                        } catch (Exception ignored) {

                        }
                    }, 30, TimeUnit.SECONDS);
                    commandExecutor.schedule(() -> {
                        try {
                            topicCoordinator.retirePartition(topicPartition);
                        } catch (Exception ignored) {
                        }
                    }, 60, TimeUnit.SECONDS);
                    promise.trySuccess(null);
                } else {
                    promise.tryFailure(future.cause());
                }
            });
        } catch (Throwable t) {
            promise.tryFailure(t);
            logger.error(t.getMessage(), t);
        }
    }

    public void attachSynchronize(Channel channel, Offset initOffset, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doAttachSynchronize(channel, initOffset, promise);
        } else {
            try {
                storageExecutor.execute(() -> {
                    doAttachSynchronize(channel, initOffset, promise);
                });
            } catch (Exception e) {
                promise.tryFailure(e);
            }
        }
    }

    private void doAttachSynchronize(Channel channel, Offset initOffset, Promise<Void> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            promise.tryFailure(RemoteException.of(Command.Failure.PROCESS_EXCEPTION, String.format(
                    "Log %d is not active now, the current state is %s", ledger, state
            )));
            return;
        }
        chunkEntryDispatcher.attach(channel, initOffset, promise);
    }

    public void cancelAllSynchronize(Promise<Void> promise) {
        try {
            chunkEntryDispatcher.cancelSubscribes();
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    public void cleanStorage() {
        storage.cleanSegment();
    }

    public void resetSubscribe(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        if (storageExecutor.inEventLoop()) {
            doResetSubscribe(channel, resetOffset, wholeMarkers, promise);
        } else {
            try {
                storageExecutor.execute(() -> doResetSubscribe(channel, resetOffset, wholeMarkers, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doResetSubscribe(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            return;
        }

        entryDispatcher.reset(channel, resetOffset, wholeMarkers, promise);
    }

    public void alterSubscribe(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        if (storageExecutor.inEventLoop()) {
            doAlterSubscribe(channel, appendMarkers, deleteMarkers, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAlterSubscribe(channel, appendMarkers, deleteMarkers, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
                logger.error(t.getMessage(), t);
            }
        }
    }

    public void doAlterSubscribe(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            return;
        }
        entryDispatcher.alter(channel, appendMarkers, deleteMarkers, promise);
    }

    public void cleanSubscribe(Channel channel, Promise<Boolean> promise) {
        if (storageExecutor.inEventLoop()) {
            doCleanSubscribe(channel, promise);
        } else {
            try {
                storageExecutor.execute(() -> doCleanSubscribe(channel, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
                logger.error(t.getMessage(), t);
            }
        }
    }

    public void doCleanSubscribe(Channel channel, Promise<Boolean> promise) {
        entryDispatcher.clean(channel, promise);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Log log = (Log) o;
        return ledger == log.ledger;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ledger);
    }

    public Offset getCurrentOffset() {
        return storage.currentOffset();
    }

    public Offset getHeadOffset() {
        return storage.headOffset();
    }

    public Offset getTailOffset() {
        return storage.tailOffset();
    }

    public LedgerCursor readFrom(Offset offset) {
        return storage.cursor(offset);
    }

    public LedgerCursor readFromHead() {
        return storage.headCursor();
    }

    public LedgerCursor readFromTail() {
        return storage.tailCursor();
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public String getTopic() {
        return topic;
    }

    public LedgerStorage getStorage() {
        return storage;
    }

    public void append(int marker, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (storageExecutor.inEventLoop()) {
            doAppend(marker, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(marker, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doAppend(int marker, ByteBuf payload, Promise<Offset> promise) {
        try {
            LogState logState = state.get();
            if (!isMigrating(logState)) {
                forwardAppendingTraffic(marker, payload, promise);
            } else if (isSynchronizing(logState)) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION,
                        String.format("The log %d can't accept appending record, current state is %s", ledger, logState)));
            } else {
                storage.appendRecord(marker, payload, promise);
            }
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getMessage(), e);
        } finally {
            payload.release();
        }
    }

    private void forwardAppendingTraffic(int marker, ByteBuf payload, Promise<Offset> promise) {
        if (migration == null) {
            storage.appendRecord(marker, payload, promise);
            return;
        }

        try {
            ClientChannel channel = migration.channel();
            SendMessageRequest request = SendMessageRequest.newBuilder()
                    .setLedger(ledger)
                    .setMarker(marker)
                    .build();
            ByteBuf data = ProtoBufUtil.protoPayloadBuf(channel.allocator(), request, payload);
            Callable<ByteBuf> callback = (v, c) -> {
                if (c == null) {
                    try {
                        SendMessageResponse response = ProtoBufUtil.readProto(v, SendMessageResponse.parser());
                        promise.trySuccess(new Offset(response.getEpoch(), response.getIndex()));
                    } catch (Throwable t) {
                        promise.tryFailure(t);
                    }
                } else {
                    promise.tryFailure(c);
                }
            };
            channel.invoke(Command.Server.SEND_MESSAGE, data, forwardTimeout, callback);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    public boolean isActive(LogState logState) {
        return isAppendable(logState) || isMigrating(logState) || isSynchronizing(logState);
    }

    public boolean isAppendable(LogState state) {
        return state == LogState.APPENDABLE;
    }

    public boolean isMigrating(LogState state) {
        return state == LogState.MIGRATING;
    }

    public boolean isSynchronizing(LogState state) {
        return state == LogState.SYNCHRONIZING;
    }

    public boolean isClosed(LogState state) {
        return state == LogState.CLOSED;
    }

    public Promise<Boolean> start(Promise<Boolean> promise) {
        Promise<Boolean> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (storageExecutor.inEventLoop()) {
            doStart(result);
        } else {
            try {
                storageExecutor.execute(() -> doStart(result));
            } catch (Exception e) {
                result.tryFailure(e);
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

    private void doStart(Promise<Boolean> promise) {
        if (state.compareAndSet(null, LogState.APPENDABLE)) {
            promise.trySuccess(true);
        } else {
            promise.trySuccess(false);
        }
    }

    public Promise<Boolean> close(Promise<Boolean> promise) {
        Promise<Boolean> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (storageExecutor.inEventLoop()) {
            doClose(result);
        } else {
            try {
                storageExecutor.execute(() -> doClose(result));
            } catch (Exception e) {
                result.tryFailure(e);
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

    private void doClose(Promise<Boolean> promise) {
        LogState logState = state.get();
        if (logState == LogState.CLOSED) {
            promise.trySuccess(false);
            return;
        }

        state.set(LogState.CLOSED);
        storage.close(null);
        entryDispatcher.close(null);
        chunkEntryDispatcher.close(null);
        Metrics.globalRegistry.remove(this.segmentBytes);
        Metrics.globalRegistry.remove(this.segmentCount);
        promise.trySuccess(true);
    }

    protected void onAppendTrigger(int ledgerId, int recordCount, Offset lastOffset) {
        entryDispatcher.dispatch();
        chunkEntryDispatcher.dispatch();
    }

    protected void onReleaseTrigger(int ledgerId, Offset oldHead, Offset newHead) {

    }

    protected void onPushMessage(int count) {
        for (LogListener listener : listeners) {
            listener.onPushMessage(topic, ledger, count);
        }
    }

    protected void onSyncMessage(int count) {
        for (LogListener listener : listeners) {
            listener.onChunkPushMessage(topic, ledger, count);
        }
    }

    public void appendChunk(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        buf.retain();
        if (storageExecutor.inEventLoop()) {
            dpAppendChunk(channel, count, buf, promise);
        } else {
            try {
                storageExecutor.submit(() -> dpAppendChunk(channel, count, buf, promise));
            } catch (Throwable t) {
                buf.release();
                promise.tryFailure(t);
            }
        }
    }

    private void dpAppendChunk(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        try {
            promise.addListener((GenericFutureListener<Future<Integer>>) future -> {
                if (future.isSuccess()) {
                    int appends = future.getNow();
                    if (logger.isWarnEnabled() && appends < count) {
                        logger.warn("Chunk append missed topic={} ledger={} exceptCount={} appendCount={}",
                                topicPartition.topic(), ledger, count, appends, future.cause());
                    }
                }
            });
            storage.appendChunkRecord(channel, count, buf, promise);
        } catch (Exception e) {
            promise.tryFailure(e);
        } finally {
            ByteBufUtil.release(buf);
        }
    }

    public void subscribeSynchronize(Channel channel, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doSubscribeSynchronize(channel, promise);
        } else {
            try {
                storageExecutor.execute(() -> doSubscribeSynchronize(channel, promise));
            } catch (Exception e) {
                promise.tryFailure(e);
            }
        }
    }

    private void doSubscribeSynchronize(Channel channel, Promise<Void> promise) {
        chunkEntryDispatcher.cancelSubscribe(channel, promise);
    }

    public class InnerTrigger implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
            onAppendTrigger(ledgerId, recordCount, lasetOffset);
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {

        }
    }

    private class InnerEntryDispatchCounter implements IntConsumer {
        @Override
        public void accept(int value) {
            onPushMessage(value);
        }
    }

    private class InnerEntryChunkDispatchCounter implements IntConsumer {
        @Override
        public void accept(int value) {
            onSyncMessage(value);
        }
    }

}
