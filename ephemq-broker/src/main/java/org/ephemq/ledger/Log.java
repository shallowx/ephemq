package org.ephemq.ledger;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntConsumer;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Offset;
import org.ephemq.common.message.TopicConfig;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.config.ServerConfig;
import org.ephemq.dispatch.ChunkDispatcher;
import org.ephemq.dispatch.DefaultDispatcher;
import org.ephemq.listener.LogListener;
import org.ephemq.metrics.config.MetricsConstants;
import org.ephemq.remote.exception.RemotingException;
import org.ephemq.remote.invoke.Callable;
import org.ephemq.remote.invoke.Command;
import org.ephemq.remote.proto.server.CancelSyncResponse;
import org.ephemq.remote.proto.server.SendMessageRequest;
import org.ephemq.remote.proto.server.SendMessageResponse;
import org.ephemq.remote.proto.server.SyncResponse;
import org.ephemq.remote.util.ByteBufUtil;
import org.ephemq.remote.util.ProtoBufUtil;
import org.ephemq.support.Manager;
import org.ephemq.zookeeper.TopicHandleSupport;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents a Log which handles various operations related to logging, synchronization,
 * migration, and state management in a distributed log system. It is responsible for appending entries,
 * managing ledger storage and partitions, handling synchronization with clients, and migrating data
 * between different destinations.
 */
public class Log {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Log.class);
    /**
     * Represents the topic and partition this log is associated with.
     * Immutable and used throughout the log to identify where entries
     * should be appended or read from.
     */
    protected final TopicPartition topicPartition;
    /**
     * The unique identifier representing a specific ledger in the log.
     * This value is assigned upon the creation of a log and remains constant
     * for the lifetime of the log instance.
     */
    protected final int ledger;
    /**
     * The topic name associated with this log. This variable is used to identify
     * the specific topic to which log entries belong.
     */
    protected final String topic;
    /**
     * Represents the storage component of the log which handles ledger storage operations.
     * It is a protected and final field, ensuring that it is only accessible within the class
     * itself or subclasses, and that its reference cannot be modified once initialized.
     */
    protected final LedgerStorage storage;
    /**
     * The executor that handles storage-related tasks and operations.
     */
    protected final EventExecutor storageExecutor;
    /**
     * Executor responsible for running command-related tasks for the log.
     * This executor is used to ensure that command operations are processed
     * asynchronously in a separate thread pool to improve performance and
     * prevent blocking the main execution thread.
     */
    protected final EventExecutor commandExecutor;
    /**
     * Entry dispatcher responsible for managing log entries processing and distribution.
     * This dispatcher handles the core mechanisms of dispatching log entries to appropriate destinations
     * and ensuring the correct sequence and integrity of log operations within the system.
     */
    protected final DefaultDispatcher entryDispatcher;
    /**
     * A list of listeners that receive notifications for various log events.
     * These listeners implement the {@link LogListener} interface and are used
     * to handle events such as log initialization, message reception, synchronization,
     * and message pushing.
     */
    protected final List<LogListener> listeners;
    /**
     * The manager responsible for handling the operational and lifecycle aspects of
     * various components like topics, clusters, logs, and connections.
     * This instance is crucial for starting, shutting down, and managing various
     * aspects of the service effectively.
     */
    protected final Manager manager;
    /**
     * Represents the metric for counting the number of segments.
     * Used for tracking and monitoring the number of segments within a log.
     */
    protected final Meter segmentCount;
    /**
     * A Meter that tracks the number of bytes written to a segment in the Log.
     */
    protected final Meter segmentBytes;
    /**
     * The timeout value in milliseconds for forwarding operations in the Log class.
     * This variable specifies the maximum duration that the system should wait for
     * an operation to complete before timing out.
     */
    protected final int forwardTimeout;
    /**
     * Represents the current state of a log.
     * <p>
     * This variable is an AtomicReference to a LogState, which could be one of the following:
     * - APPENDABLE
     * - SYNCHRONIZING
     * - MIGRATING
     * - CLOSED
     * <p>
     * Initial state is set to null.
     */
    protected final AtomicReference<LogState> state = new AtomicReference<>(null);
    /**
     * A dispatcher responsible for processing chunk entries within the log.
     * It likely handles the task of receiving, queuing, and dispatching chunk entries
     * to the appropriate consumers or handlers.
     */
    protected ChunkDispatcher chunkEntryDispatcher;
    /**
     * Manages the state and operations related to the migration process for the Log.
     * This involves migrating data to a destination channel and keeping track of the ledger being migrated.
     * The migration can be triggered and managed via various methods in the Log class.
     */
    protected Migration migration;
    /**
     * The dedicated channel for synchronizing logs with other replicas or targets.
     * This channel handles the communication required to ensure log consistency
     * across distributed systems during synchronization processes.
     */
    protected ClientChannel syncChannel;
    /**
     * A protected Promise instance that holds the response for a synchronization
     * operation in the Log class. This promise is used internally to manage the
     * asynchronous handling of the SyncResponse.
     */
    protected Promise<SyncResponse> syncPromise;
    /**
     * A promise used to handle cancellation of synchronization processes.
     * This promise is associated with a response that confirms the cancellation.
     */
    protected Promise<CancelSyncResponse> cancelSyncPromise;

    /**
     * Constructs a new Log instance with the specified configurations.
     *
     * @param config Server configuration settings.
     * @param topicPartition The topic and partition information.
     * @param ledger The ledger id.
     * @param epoch The epoch.
     * @param manager The manager responsible for handling various operations and configurations.
     * @param topicConfig Topic-specific configurations, may be null.
     */
    public Log(ServerConfig config, TopicPartition topicPartition, int ledger, int epoch, Manager manager,
               TopicConfig topicConfig) {
        this.topicPartition = topicPartition;
        this.ledger = ledger;
        this.topic = topicPartition.topic();
        this.forwardTimeout = config.getRecordDispatchConfig().getDispatchEntryFollowLimit();
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
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
        this.storageExecutor = manager.getMessageStorageEventExecutorGroup().next();
        this.storage = new LedgerStorage(ledger, topicPartition.topic(), epoch, ledgerConfig, storageExecutor, new InnerTrigger());
        this.manager = manager;
        this.listeners = manager.getLogHandler().getLogListeners();
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
        this.entryDispatcher = new DefaultDispatcher(ledger, topic, storage, config.getRecordDispatchConfig(),
                manager.getMessageDispatchEventExecutorGroup(), new InnerEntryDispatchCounter());
        this.chunkEntryDispatcher =
                new ChunkDispatcher(ledger, topic, storage, config.getChunkRecordDispatchConfig(),
                        manager.getMessageDispatchEventExecutorGroup(), new InnerEntryChunkDispatchCounter());
    }

    /**
     * Retrieves the synchronized client channel associated with this log.
     *
     * @return the synchronized client channel.
     */
    public ClientChannel getSyncChannel() {
        return syncChannel;
    }

    /**
     * Returns the identifier of the ledger associated with this log.
     *
     * @return the ledger identifier.
     */
    public int getLedger() {
        return ledger;
    }

    /**
     * Returns the current epoch of the ledger storage.
     *
     * @return the current epoch.
     */
    public int getEpoch() {
        return storage.currentOffset().getEpoch();
    }

    /**
     * Updates the epoch value for the current log instance.
     *
     * @param epoch the new epoch value to be set
     */
    public void updateEpoch(int epoch) {
        storage.updateEpoch(epoch);
    }

    /**
     * Initiates the migration process to a specified destination.
     *
     * @param dest the destination to migrate to
     * @param destChannel the client channel associated with the destination
     * @param promise a promise that will be completed when the migration is finished
     */
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

    /**
     * Retrieves the total count of subscribers by summing up the channel counts
     * from the entry dispatcher and the chunk entry dispatcher.
     *
     * @return the total number of subscribers.
     */
    public int getSubscriberCount() {
        return entryDispatcher.channelCount() + chunkEntryDispatcher.channelCount();
    }

    /**
     * Synchronizes data from the target ClientChannel starting at the specified offset with a given timeout.
     *
     * @param clientChannel The ClientChannel from which to synchronize data.
     * @param offset The starting offset for the synchronization.
     * @param timeoutMs The timeout duration in milliseconds for the synchronization.
     * @return A Promise that will be completed with a SyncResponse when the synchronization is done.
     */
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

    /**
     * Synchronizes data from the target client channel starting from the given offset.
     *
     * @param clientChannel the client channel to synchronize from
     * @param offset the offset from which synchronization should start
     * @param timeoutMs the timeout value in milliseconds
     * @param promise the promise to be fulfilled with the synchronization response
     */
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
            promise.tryFailure(RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                    String.format("The log can't begin to sync data,the current state is %s}", state)));

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
            TopicHandleSupport support = manager.getTopicHandleSupport();
            support.getParticipantSuooprt()
                    .syncLeader(topicPartition, ledger, clientChannel, syncOffset.getEpoch(), syncOffset.getIndex(),
                            timeoutMs, syncPromise);
        } catch (Exception e) {
            syncPromise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Requests a cancellation of an ongoing synchronization process with a specified timeout.
     *
     * @param timeoutMs the time in milliseconds to wait for the cancellation to complete.
     * @return a Promise that will be completed when the cancellation is done.
     */
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

    /**
     * Cancels the ongoing synchronization process and resets the internal state to APPENDABLE,
     * if the current state allows cancellation.
     *
     * @param timeoutMs The timeout in milliseconds for the cancellation request.
     * @param promise The promise to be fulfilled with the result of the cancellation process.
     */
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
            TopicHandleSupport support = manager.getTopicHandleSupport();
            support.getParticipantSuooprt().unSyncLedger(topicPartition, ledger, syncChannel, timeoutMs, unSyncPromise);
        } catch (Exception e) {
            unSyncPromise.tryFailure(e);
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Initiates a migration process to the specified destination using the provided client channel.
     * The operation involves syncing data from the destination channel and scheduling partition
     * handover and retirement tasks.
     *
     * @param dest The target destination for migration.
     * @param destChannel The client channel associated with the destination.
     * @param promise A promise that will be completed upon the success or failure of the migration.
     */
    private void doMigrate(String dest, ClientChannel destChannel, Promise<Void> promise) {
        LogState logState = state.get();
        if (!isAppendable(logState)) {
            return;
        }

        migration = new Migration(ledger, destChannel);
        state.set(LogState.MIGRATING);
        try {
            TopicHandleSupport support = manager.getTopicHandleSupport();
            Promise<SyncResponse> syncResponsePromise = syncFromTarget(destChannel, new Offset(0, 0L), 30000);
            syncResponsePromise.addListener(future -> {
                if (future.isSuccess()) {
                    commandExecutor.schedule(() -> {
                        try {
                            support.handoverPartition(dest, topicPartition);
                        } catch (Exception ignored) {

                        }
                    }, 30, TimeUnit.SECONDS);
                    commandExecutor.schedule(() -> {
                        try {
                            support.retirePartition(topicPartition);
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

    /**
     * Attaches synchronization for the given channel starting from the specified offset.
     *
     * @param channel    the channel to attach for synchronization
     * @param initOffset the initial offset to start synchronization from
     * @param promise    the promise to be completed when the operation is done
     */
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

    /**
     *
     */
    private void doAttachSynchronize(Channel channel, Offset initOffset, Promise<Void> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            promise.tryFailure(RemotingException.of(Command.Failure.PROCESS_EXCEPTION, String.format("Log[ledger:%s] is not active now, because of the current state is %s", ledger, logState)));
            return;
        }
        chunkEntryDispatcher.attach(channel, initOffset, promise);
    }

    /**
     * Cancels all ongoing synchronization processes and completes the given promise.
     *
     * @param promise the promise that will be completed when all synchronization processes are canceled.
     */
    public void cancelAllSynchronize(Promise<Void> promise) {
        try {
            chunkEntryDispatcher.cancelSubscribes();
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    /**
     * Cleans the current storage segment in the log.
     * <p>
     * This method triggers the cleaning process of the current storage segment
     * by invoking the cleanSegment method on the storage object.
     */
    public void cleanStorage() {
        storage.cleanSegment();
    }

    /**
     * Resets the subscription for a given channel to a specified offset.
     *
     * @param channel       The channel for which the subscription is being reset.
     * @param resetOffset   The offset to which the subscription should be reset.
     * @param wholeMarkers  A collection of markers that represent the reset state.
     * @param promise       A promise that will be fulfilled with the outcome of the reset operation.
     */
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

    /**
     * Resets the subscription for a given channel to a specific offset with a set of markers.
     *
     * @param channel the channel for which the subscription needs to be reset.
     * @param resetOffset the offset to which the subscription should be reset.
     * @param wholeMarkers the collection of markers to be applied after resetting the offset.
     * @param promise the promise representing the outcome of the reset operation.
     */
    private void doResetSubscribe(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            return;
        }

        entryDispatcher.reset(channel, resetOffset, wholeMarkers, promise);
    }

    /**
     * Alters the subscription for the provided channel by appending and deleting markers.
     * This method ensures the operation is executed on the appropriate event loop.
     *
     * @param channel the channel to alter the subscription for
     * @param appendMarkers the collection of markers to append to the subscription
     * @param deleteMarkers the collection of markers to delete from the subscription
     * @param promise the promise to be completed with the result of the operation
     */
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

    /**
     * Alters the subscription state of the provided channel by appending and deleting marker sets.
     *
     * @param channel The channel whose subscription state is to be altered.
     * @param appendMarkers The markers to be appended to the channel's subscription.
     * @param deleteMarkers The markers to be deleted from the channel's subscription.
     * @param promise A promise to indicate the success or failure of the operation.
     */
    public void doAlterSubscribe(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isActive(logState)) {
            return;
        }
        entryDispatcher.alter(channel, appendMarkers, deleteMarkers, promise);
    }

    /**
     * Cleans the subscription data for the specified channel.
     *
     * @param channel the channel for which the subscription needs to be cleaned
     * @param promise the promise to be completed once the cleaning operation is done
     */
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

    /**
     * Initiates a clean-up operation on the specified channel and completes
     * the provided promise with the result of the operation.
     *
     * @param channel the channel to be cleaned
     * @param promise the promise to be completed with the result of the clean-up operation
     */
    public void doCleanSubscribe(Channel channel, Promise<Boolean> promise) {
        entryDispatcher.clean(channel, promise);
    }

    /**
     * Compares this Log instance to the specified object and determines if they are equal.
     *
     * @param o the object to be compared for equality with this Log instance
     * @return true if the specified object is equal to this Log instance, otherwise false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Log log = (Log) o;
        return ledger == log.ledger;
    }

    /**
     * Computes a hash code for the Log object using its ledger field.
     *
     * @return the computed hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hash(ledger);
    }

    /**
     * Retrieves the current offset in the ledger storage.
     *
     * @return the current offset where new records can be appended in the ledger.
     */
    public Offset getCurrentOffset() {
        return storage.currentOffset();
    }

    /**
     * Retrieves the head offset from the ledger storage.
     *
     * @return the Offset representing the head position in the ledger storage.
     */
    public Offset getHeadOffset() {
        return storage.headOffset();
    }

    /**
     * Retrieves the tail offset from the ledger storage.
     *
     * @return the tail offset from the ledger storage.
     */
    public Offset getTailOffset() {
        return storage.tailOffset();
    }

    /**
     * Reads from the ledger starting at the given offset.
     *
     * @param offset the starting point from where to read in the ledger.
     * @return a LedgerCursor positioned at the specified offset in the ledger.
     */
    public LedgerCursor readFrom(Offset offset) {
        return storage.cursor(offset);
    }

    /**
     * Returns a LedgerCursor instance that points to the head of the storage.
     *
     * @return a LedgerCursor positioned at the head of the storage
     */
    public LedgerCursor readFromHead() {
        return storage.headCursor();
    }

    /**
     * Returns a cursor that allows reading entries from the tail of the ledger.
     *
     * @return A LedgerCursor positioned at the tail of the ledger.
     */
    public LedgerCursor readFromTail() {
        return storage.tailCursor();
    }

    /**
     * Retrieves the TopicPartition associated with the log.
     *
     * @return the TopicPartition object representing the topic and partition of the log.
     */
    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    /**
     * Retrieves the topic associated with this log.
     *
     * @return the topic as a String.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Retrieves the LedgerStorage instance associated with this Log.
     *
     * @return the LedgerStorage instance linked with this Log.
     */
    public LedgerStorage getStorage() {
        return storage;
    }

    /**
     * Appends a payload to the log with the given marker. If the operation is outside the event loop,
     * it is executed asynchronously.
     *
     * @param marker the marker associated with the payload
     * @param payload the payload to append
     * @param promise the promise that will be fulfilled with the Offset of the appended record
     */
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

    /**
     * Appends a record to the log or forwards it, based on the current log state.
     *
     * @param marker an integer value that identifies the record to be appended
     * @param payload the data to be appended, encapsulated in a ByteBuf
     * @param promise a promise that will be completed with the result of the append operation
     */
    private void doAppend(int marker, ByteBuf payload, Promise<Offset> promise) {
        try {
            LogState logState = state.get();
            if (!isMigrating(logState)) {
                forwardAppendingTraffic(marker, payload, promise);
            } else if (isSynchronizing(logState)) {
                promise.tryFailure(RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                        String.format("Log[ledger:%s}] can't accept appending record, because of the current state is %s", ledger, logState)));

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

    /**
     * Handles the forwarding of appending traffic either to the local storage or to a remote migration channel.
     * <p>
     * If the migration is not active, the data is appended to the local storage.
     * Otherwise, it forwards the data to a specified client channel along with handling the related promises and callbacks.
     *
     * @param marker An integer marker indicating the type of the record.
     * @param payload A {@link ByteBuf} containing the data to be appended.
     * @param promise A {@link Promise} to be completed with the new {@link Offset} of the appended record, or with an error if the operation fails.
     */
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

    /**
     * Determines if the log is active based on its current state.
     *
     * @param logState the current state of the log.
     * @return {@code true} if the log is in appendable, migrating, or synchronizing state;
     *         {@code false} otherwise.
     */
    public boolean isActive(LogState logState) {
        return isAppendable(logState) || isMigrating(logState) || isSynchronizing(logState);
    }

    /**
     * Determines if the log state is appendable.
     *
     * @param state The current state of the log.
     * @return true if the state is APPENDABLE; false otherwise.
     */
    public boolean isAppendable(LogState state) {
        return state == LogState.APPENDABLE;
    }

    /**
     * Checks if the Log is in the MIGRATING state.
     *
     * @param state the current state of the Log.
     * @return true if the Log is in the MIGRATING state, false otherwise.
     */
    public boolean isMigrating(LogState state) {
        return state == LogState.MIGRATING;
    }

    /**
     * Checks if the given log state is in the SYNCHRONIZING state.
     *
     * @param state the state of the log to be checked
     * @return true if the log state is SYNCHRONIZING, false otherwise
     */
    public boolean isSynchronizing(LogState state) {
        return state == LogState.SYNCHRONIZING;
    }

    /**
     * Checks if the LogState is set to CLOSED.
     *
     * @param state the current state of the log
     * @return true if the state is CLOSED, false otherwise
     */
    public boolean isClosed(LogState state) {
        return state == LogState.CLOSED;
    }

    /**
     * Method to start a task, which may initialize a new promise if none is provided,
     * and execute the task either immediately or asynchronously depending on the state
     * of the storage executor's event loop.
     *
     * @param promise a promise that will be used to indicate the start of the task. If null, a new promise will be created.
     * @return a promise indicating whether the task was successfully started.
     */
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

    /**
     * Initiates the start process by setting the log state to APPENDABLE if it is null.
     *
     * @param promise the promise to notify when the start process is complete, either successfully or unsuccessfully
     */
    private void doStart(Promise<Boolean> promise) {
        if (state.compareAndSet(null, LogState.APPENDABLE)) {
            promise.trySuccess(true);
        } else {
            promise.trySuccess(false);
        }
    }

    /**
     * Closes the log and completes the specified promise with the result.
     *
     * @param promise The promise to be completed once the close operation finishes.
     *                If null, a new promise will be created and used.
     * @return The promise that will be completed with the result of the close operation.
     */
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

    /**
     * Closes the log if it is not already closed and updates the state, storage,
     * entry dispatcher, and chunk entry dispatcher accordingly.
     * Updates metrics and fulfills the provided promise based on the result.
     *
     * @param promise A Promise object that will be completed with a Boolean value indicating
     *                whether the log was successfully closed or if it was already closed.
     */
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

    /**
     * Triggered upon appending a new record to a ledger.
     * This method dispatches events to the registered handlers and
     * chunk handlers for further processing.
     *
     * @param ledgerId    the unique identifier of the ledger to which a record was appended
     * @param recordCount the total number of records appended
     * @param lastOffset  the offset of the last record appended
     */
    protected void onAppendTrigger(int ledgerId, int recordCount, Offset lastOffset) {
        entryDispatcher.dispatch();
        chunkEntryDispatcher.dispatch();
    }

    /**
     * Handles the event when a release trigger is fired.
     *
     * @param ledgerId the identifier of the ledger.
     * @param oldHead the previous head offset.
     * @param newHead the new head offset.
     */
    protected void onReleaseTrigger(int ledgerId, Offset oldHead, Offset newHead) {

    }

    /**
     * Notifies all registered {@code LogListener}s about the incoming push message.
     *
     * @param count the number of messages to push to the listeners
     */
    protected void onPushMessage(int count) {
        for (LogListener listener : listeners) {
            listener.onPushMessage(topic, ledger, count);
        }
    }

    /**
     * Handles synchronization messages by notifying all registered listeners.
     *
     * @param count The number of messages in the synchronization process.
     */
    protected void onSyncMessage(int count) {
        for (LogListener listener : listeners) {
            listener.onChunkPushMessage(topic, ledger, count);
        }
    }

    /**
     * Appends a chunk of data to the storage for the specified channel and count, using the given buffer and promise.
     *
     * @param channel the Channel through which the data is being transmitted
     * @param count the number of chunks to be appended
     * @param buf the ByteBuf containing the data to be appended
     * @param promise the Promise that will be completed once the append operation is finished or fails
     */
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

    /**
     * Internal method to append a chunk of records to the ledger storage.
     * Handles promise completion and error logging.
     *
     * @param channel The channel through which the record is received.
     * @param count The number of records to be appended.
     * @param buf The buffer containing the record data.
     * @param promise The promise to be completed once the append operation is finished.
     */
    private void dpAppendChunk(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        try {
            promise.addListener((GenericFutureListener<Future<Integer>>) future -> {
                if (future.isSuccess()) {
                    int appends = future.getNow();
                    if (logger.isWarnEnabled() && appends < count) {
                        logger.warn("[:: topic:{}, ledger:{}, exceptCount:{}, appendCount:{}]Chunk append missed",
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

    /**
     * Subscribes a channel for synchronization.
     *
     * @param channel the channel to be subscribed for synchronization
     * @param promise the promise to be completed when the subscribe operation finishes
     */
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

    /**
     * Performs a synchronized subscription operation by canceling the existing subscription
     * associated with the given channel and promise.
     *
     * @param channel the channel whose subscription is to be canceled.
     * @param promise the promise to be notified once the subscription is canceled.
     */
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
