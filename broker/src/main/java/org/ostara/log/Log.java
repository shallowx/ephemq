package org.ostara.log;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.ostara.client.internal.ClientChannel;
import org.ostara.common.Offset;
import org.ostara.common.TopicConfig;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.listener.LogListener;
import org.ostara.log.ledger.LedgerConfig;
import org.ostara.log.ledger.LedgerCursor;
import org.ostara.log.ledger.LedgerStorage;
import org.ostara.log.ledger.LedgerTrigger;
import org.ostara.management.Manager;
import org.ostara.management.TopicManager;
import org.ostara.metrics.MetricsConstants;
import org.ostara.remote.RemoteException;
import org.ostara.remote.proto.server.CancelSyncResponse;
import org.ostara.remote.proto.server.SyncResponse;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Log {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Log.class);
    private Config config;
    private TopicPartition topicPartition;
    private int ledger;
    private String topic;
    private LedgerStorage storage;
    private EventExecutor storageExecutor;
    private EventExecutor commandExecutor;
    private RecordEntryDispatcher entryDispatcher;
    private RecordChunkDispatcher chunkDispatcher;
    private List<LogListener> listeners;
    private Manager manager;
    private Meter segmentCountMeter;
    private Meter segmentBytesMeter;
    private int forwardTimeout = 100;
    private AtomicReference<LogState> state = new AtomicReference<>(null);
    private Migration migration;
    private ClientChannel syncChannel;
    private Promise<SyncResponse> syncFuture;
    private Promise<CancelSyncResponse> unSyncFuture;

    public Log(Config config, TopicPartition topicPartition, int ledger, int epoch, Manager manager, TopicConfig topicConfig) {
        this.config = config;
        this.topicPartition = topicPartition;
        this.ledger = ledger;
        this.topic = topicPartition.getTopic();
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
        LedgerConfig ledgerConfig;
        if (topicConfig != null) {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(topicConfig.getSegmentRetainCount())
                    .segmentBufferCapacity(topicConfig.getSegmentRollingSize())
                    .segmentRetainMs(topicConfig.getSegmentRetainMs());
        } else {
            ledgerConfig = new LedgerConfig()
                    .segmentRetainCounts(config.getSegmentRetainCounts())
                    .segmentBufferCapacity(config.getSegmentRollingSize())
                    .segmentRetainMs(config.getSegmentRetainTime());
        }

        storageExecutor = manager.getMessageStorageEventExecutorGroup().next();
        this.storage = new LedgerStorage(ledger, topicPartition.getTopic(), epoch, ledgerConfig, storageExecutor, new InnerTrigger());
        this.manager = manager;
        this.listeners = manager.getLogManager().getLogListeners();
        Tags tags = Tags.of(MetricsConstants.TOPIC_TAG, topicPartition.getTopic())
                .and(MetricsConstants.PARTITION_TAG, String.valueOf(topicPartition.getPartition()))
                .and(MetricsConstants.BROKER_TAG, config.getServerId())
                .and(MetricsConstants.CLUSTER_TAG, config.getClusterName())
                .and(MetricsConstants.LEDGER_TAG, Integer.toString(ledger));

        this.segmentCountMeter = Gauge.builder(MetricsConstants.LOG_SEGMENT_COUNT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentCount)
                .tags(tags).register(Metrics.globalRegistry);

        this.segmentBytesMeter = Gauge.builder(MetricsConstants.LOG_SEGMENT_GAUGE_NAME, this.getStorage(), LedgerStorage::segmentBytes)
                .baseUnit("bytes")
                .tags(tags).register(Metrics.globalRegistry);

       this.entryDispatcher = new RecordEntryDispatcher(ledger, topic, storage, config, manager.getMessageDispatchEventExecutorGroup(), new InnerEntryDispatchCounter());
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
            } catch (Exception e){
                promise.tryFailure(e);
            }
        }
    }

    public int getSubscriberCount() {
        return entryDispatcher.channelCount();
    }

    public Promise<SyncResponse> syncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs) {
        Promise<SyncResponse> result = storageExecutor.newPromise();
        if (storageExecutor.inEventLoop()) {
             doSyncFromTarget(clientChannel, offset, timeoutMs, result);
        } else {
            try {
                storageExecutor.submit(() -> doSyncFromTarget(clientChannel, offset, timeoutMs, result));
            } catch (Throwable t){
                result.tryFailure(t);
            }
        }
        return result;
    }

    private void doSyncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs, Promise<SyncResponse> promise) {
        if (syncFuture != null && !syncFuture.isDone()) {
            syncFuture.addListener(f -> doSyncFromTarget(clientChannel, offset, timeoutMs, promise));
            return;
        }

        if (unSyncFuture != null && !unSyncFuture.isDone()) {
            unSyncFuture.addListener(f -> doSyncFromTarget(clientChannel, offset, timeoutMs, promise));
            return;
        }

        syncFuture = promise;
        promise.addListener(future -> syncFuture = null);
        LogState logState = state.get();
        if (!isAppendAble(logState) && !isMigrating(logState)) {
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
            }

            promise.tryFailure(f.cause());
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
            TopicManager topicManager = manager.getTopicManager();
            topicManager.getReplicaManager().syncLeader(topicPartition, ledger, clientChannel, syncOffset.getEpoch(), syncOffset.getIndex(), timeoutMs, syncPromise);
        } catch (Exception e) {
            syncPromise.tryFailure(e);
        }
    }

    public Promise<CancelSyncResponse> unSync(int timeoutMs) {
        Promise<CancelSyncResponse> promise = storageExecutor.newPromise();
        if (storageExecutor.inEventLoop()) {
            doUnSync(timeoutMs, promise);
        } else {
            try {
                storageExecutor.submit(() ->  doUnSync(timeoutMs, promise));
            } catch (Exception e) {
                promise.tryFailure(e);
            }
        }
        return promise;
    }

    private void doUnSync(int timeoutMs, Promise<CancelSyncResponse> promise) {
        if (syncFuture != null && !syncFuture.isDone()) {
            syncFuture.addListener(future -> doUnSync(timeoutMs, promise));
            return;
        }

        if (unSyncFuture != null && !unSyncFuture.isDone()) {
            unSyncFuture.addListener(future -> doUnSync(timeoutMs, promise));
            return;
        }

        unSyncFuture = promise;
        promise.addListener(future -> unSyncFuture = null);
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
            TopicManager topicManager = manager.getTopicManager();
            topicManager.getReplicaManager().unSyncLedger(topicPartition, ledger, syncChannel, timeoutMs, unSyncPromise);
        } catch (Exception e){
            unSyncPromise.tryFailure(e);
        }
    }

    private void doMigrate(String dest, ClientChannel destChannel, Promise<Void> promise) {
        LogState logState = state.get();
        if (!isAppedable(logState)) {
            return;
        }

        migration = new Migration(ledger, destChannel);
        state.set(LogState.MIGRATING);
        try {
            TopicManager topicManager = manager.getTopicManager();
            Promise<SyncResponse> syncResponsePromise = syncFromTarget(destChannel, new Offset(0, 0L), 30000);
            syncResponsePromise.addListener(future -> {
                if(future.isSuccess()) {
                    commandExecutor.schedule(() -> {
                        try {
                            topicManager.handoverPartition(dest, topicPartition);
                        } catch (Exception e){

                        }
                    }, 30 , TimeUnit.SECONDS);
                    commandExecutor.schedule(() -> {
                        try {
                            topicManager.retirePartition(topicPartition);
                        } catch (Exception e){

                        }
                    }, 60 , TimeUnit.SECONDS);
                    promise.trySuccess(null)
                } else {
                    promise.tryFailure(future.cause());
                }
            });
        } catch (Throwable t) {
            promise.tryFailure(t);
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
            }
        }
    }
    public void doCleanSubscribe(Channel channel, Promise<Boolean> promise) {
        entryDispatcher.clean(channel, promise);
    }

    public void attachSynchronize(Channel channel, Offset offset, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doAttachSynchronize(channel, offset, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAttachSynchronize(channel, offset, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
    }
    public void doAttachSynchronize(Channel channel, Offset offset,  Promise<Void> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            return;
        }
        entryDispatcher.attach(channel, offset, promise);
    }

    public void detachSynchronize(Channel channel, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doDetachSynchronize(channel, promise);
        } else {
            try {
                storageExecutor.execute(() -> doDetachSynchronize(channel, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
    }
    public void doDetachSynchronize(Channel channel, Offset offset,  Promise<Void> promise) {
        chunkDispatcher.attach(channel, offset, promise);
    }

    public void detachAllSynchronize(Promise<Void> promise) {
        try {
            chunkDispatcher.detachAll(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
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

    public Offset getHeadOffset(){
        return storage.headOffset();
    }

    public Offset tailOffset() {
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

    public class InnerTrigger implements LedgerTrigger{
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {

        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {

        }
    }
}
