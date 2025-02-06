package org.meteor.proxy.core;

import io.netty.channel.Channel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.IntList;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogState;
import org.meteor.listener.TopicListener;
import org.meteor.proxy.MeteorProxy;
import org.meteor.remote.proto.MessageOffset;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.support.Manager;

import java.util.concurrent.atomic.LongAdder;

/**
 * This class represents a proxy log used to manage and synchronize the logging
 * operations between different components. It extends the base `Log` class
 * adding additional proxy-specific functionalities.
 */
public class ProxyLog extends Log {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    /**
     * A counter that keeps track of the total number of messages dispatched by the ProxyLog.
     * This counter is updated atomically and helps in monitoring the message dispatching
     * performance and reliability of the ProxyLog.
     */
    private final LongAdder totalDispatchedMessages = new LongAdder();
    /**
     * Configuration for the proxy server.
     */
    private final ProxyConfig proxyConfiguration;
    /**
     * Stores the timestamp of the last time a subscription was made.
     * <p>
     * This variable is used to track the last time a subscription event occurred
     * in the ProxyLog class, ensuring that various time-based operations can be
     * accurately recorded and monitored.
     * <p>
     * The `volatile` keyword ensures that updates to this variable are immediately
     * visible to all threads, which is important in a multi-threaded environment.
     * <p>
     * Initialized to the current system time in milliseconds when the ProxyLog instance is created.
     */
    private volatile long lastSubscribeTimeMillis = System.currentTimeMillis();
    /**
     * Represents the current location of the tail in the stream.
     * This variable is volatile to ensure visibility across threads.
     * The tail location is defined using an {@link Offset} instance,
     * which includes an epoch and an index.
     */
    private volatile Offset tailLocation = new Offset(0, 0L);
    /**
     * Represents the earliest location in the log stream from which the ProxyLog
     * must read data. It is specified as an Offset, which includes an epoch and an index.
     * This variable is declared as volatile to ensure visibility of changes across
     * multiple threads, enabling safe concurrent access.
     */
    private volatile Offset headLocation = new Offset(0, 0L);

    /**
     * Constructs a ProxyLog instance.
     *
     * @param config          The configuration for the proxy server.
     * @param topicPartition  The partition of the topic that this log is associated with.
     * @param ledger          The ledger identifier.
     * @param epoch           The epoch number.
     * @param manager         The manager responsible for handling the server components and interactions.
     * @param topicConfig     The configuration specific to the topic.
     */
    public ProxyLog(ProxyServerConfig config, TopicPartition topicPartition, int ledger, int epoch, Manager manager,
                    TopicConfig topicConfig) {
        super(config, topicPartition, ledger, epoch, manager, topicConfig);
        this.proxyConfiguration = config.getProxyConfiguration();
    }

    /**
     * Cancels any ongoing synchronization and closes the log if there are no remaining subscribers.
     *
     * @param promise a promise that will be fulfilled with the result of the operation.
     * If the operation is successful, the promise will be completed with {@code true} if the log was closed,
     * and {@code false} otherwise. If the operation fails, the promise will be completed with a failure.
     */
    public void cancelSyncAndCloseIfNotSubscribe(Promise<Boolean> promise) {
        if (storageExecutor.inEventLoop()) {
            doCancelSyncAndCloseIfNotSubscribe(promise);
        } else {
            try {
                storageExecutor.execute(() -> doCancelSyncAndCloseIfNotSubscribe(promise));
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(e.getMessage(), e);
                }
                promise.tryFailure(e);
            }
        }
    }

    /**
     * Attempts to cancel synchronization and close the log if there are no active subscribers.
     * The method first checks the number of subscribers. If there are any, it resolves the promise
     * with a value of false. If the log is already closed, it resolves the promise with false.
     * If the log is currently synchronizing, it attempts to cancel synchronization before closing
     * the log and then resolves the promise. Otherwise, it directly attempts to close the log.
     *
     * @param promise the promise to be resolved based on the outcome of the cancellation and closure attempts
     */
    private void doCancelSyncAndCloseIfNotSubscribe(Promise<Boolean> promise) {
        if (getSubscriberCount() > 0) {
            promise.trySuccess(false);
            return;
        }
        LogState logState = state.get();
        if (isClosed(logState)) {
            promise.trySuccess(false);
            return;
        }

        if (isSynchronizing(logState)) {
            Promise<CancelSyncResponse> cancelSyncPromise = cancelSync(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
            cancelSyncPromise.addListener(f -> {
                if (f.isSuccess()) {
                    Promise<Boolean> closePromise = storageExecutor.newPromise();
                    closePromise.addListener(future -> {
                        if (future.isSuccess()) {
                            promise.trySuccess((Boolean) future.get());
                            for (TopicListener listener : manager.getTopicHandleSupport().getTopicListener()) {
                                listener.onPartitionDestroy(topicPartition, ledger);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    });
                    close(closePromise);
                } else {
                    promise.tryFailure(f.cause());
                }
            });
            return;
        }

        Promise<Boolean> closePromise = storageExecutor.newPromise();
        closePromise.addListener(f -> {
            if (f.isSuccess()) {
                promise.trySuccess((Boolean) f.get());
                for (TopicListener listener : manager.getTopicHandleSupport().getTopicListener()) {
                    listener.onPartitionDestroy(topicPartition, ledger);
                }
            } else {
                promise.tryFailure(f.cause());
            }
        });
        close(closePromise);
    }

    /**
     * Synchronizes the log and resets the subscription based on the provided parameters.
     *
     * @param syncChannel The ClientChannel used for synchronization.
     * @param epoch The epoch number for versioning.
     * @param index The index position in the log.
     * @param channel The Netty Channel for communication.
     * @param markerSet An IntList containing markers for the operation.
     * @param promise A Promise to be fulfilled with the result of the operation.
     */
    public void syncAndResetSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, IntList markerSet, Promise<Integer> promise) {
        Promise<Integer> ret = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (storageExecutor.inEventLoop()) {
            doSyncAndResetSubscribe(syncChannel, epoch, index, channel, markerSet, promise);
        } else {
            try {
                storageExecutor.execute(() -> {
                    doSyncAndResetSubscribe(syncChannel, epoch, index, channel, markerSet, promise);
                });
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(e.getMessage(), e);
                }
                ret.tryFailure(e);
            }
        }
    }

    /**
     * Synchronizes the target and resets the subscription based on the provided parameters.
     *
     * @param syncChannel the channel used for synchronization
     * @param epoch the epoch number to locate the subscription
     * @param index the index to locate the subscription
     * @param channel the channel where the subscription needs to be reset
     * @param markerSet the set of markers used during the reset process
     * @param promise the promise to be fulfilled with the result of the reset operation
     */
    private void doSyncAndResetSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, IntList markerSet, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isSynchronizing(logState)) {
            Promise<SyncResponse> syncPromise = syncFromTarget(syncChannel, new Offset(0, 0L), proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
            syncPromise.addListener(f -> {
                if (f.isSuccess()) {
                    Offset locate = locate(Offset.of(epoch, index));
                    resetSubscribe(channel, locate, markerSet, promise);
                } else {
                    promise.tryFailure(f.cause());
                }
            });
        } else {
            if (syncPromise != null && !syncPromise.isDone()) {
                syncPromise.addListener(future -> {
                    if (storageExecutor.inEventLoop()) {
                        resetSubscribe(channel, locate(Offset.of(epoch, index)), markerSet, promise);
                    } else {
                        try {
                            storageExecutor.execute(() -> {
                                resetSubscribe(channel, locate(Offset.of(epoch, index)), markerSet, promise);
                            });
                        } catch (Exception e) {
                            promise.tryFailure(e);
                        }
                    }
                });
            } else {
                resetSubscribe(channel, locate(Offset.of(epoch, index)), markerSet, promise);
            }
        }
        setLastSubscribeTimeMs();
    }

    /**
     * Subscribes to a sync and chunk process on a given client channel.
     *
     * @param syncChannel The client channel to synchronize with.
     * @param epoch The epoch number for the subscription.
     * @param index The index at which to start subscribing.
     * @param channel The channel on which to perform the subscription.
     * @param promise The promise object for handling the sync response asynchronously. Can be null.
     */
    public void syncAndChunkSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Promise<SyncResponse> ret = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (storageExecutor.inEventLoop()) {
            doSyncAndChunkSubscribe(syncChannel, epoch, index, channel, promise);
        } else {
            try {
                storageExecutor.execute(() -> {
                    doSyncAndChunkSubscribe(syncChannel, epoch, index, channel, promise);
                });
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(e.getMessage(), e);
                }
                ret.tryFailure(e);
            }
        }
    }

    /**
     * Synchronizes the current state and subscribes to the designated chunk using the provided channels and index.
     *
     * @param syncChannel - The channel used for synchronization.
     * @param epoch - The epoch value indicating the current time period in the event sequence.
     * @param index - The index value specifying the current position within the epoch.
     * @param channel - The channel used for subscribing to updates.
     * @param promise - The promise object for handling the SyncResponse.
     */
    private void doSyncAndChunkSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Promise<Void> vp = getVoidPromise(promise);
        LogState logState = state.get();
        if (!isSynchronizing(logState)) {
            Promise<SyncResponse> syncPromise = syncFromTarget(syncChannel, new Offset(0, 0L), proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds());
            syncPromise.addListener(future -> {
                if (future.isSuccess()) {
                    attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                } else {
                    promise.tryFailure(future.cause());
                }
            });
        } else {
            if (syncPromise != null && !syncPromise.isDone()) {
                syncPromise.addListener(future -> {
                    if (storageExecutor.inEventLoop()) {
                        attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                    } else {
                        try {
                            storageExecutor.execute(() -> {
                                attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                            });
                        } catch (Exception e) {
                            promise.tryFailure(e);
                        }
                    }
                });
            } else {
                attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
            }
        }
        setLastSubscribeTimeMs();
    }

    /**
     * Returns a Promise that represents a completion of a synchronous process
     * involving the provided `SyncResponse` promise. This method creates and
     * returns a new Void promise, and attaches a listener to it, which upon
     * successful completion, updates the SyncResponse promise with information
     * including headOffset, tailOffset, and currentOffset.
     *
     * @param promise the SyncResponse Promise that will be updated upon successful completion
     * @return a new Promise of type Void that represents the initial promise for the operation
     */
    private Promise<Void> getVoidPromise(Promise<SyncResponse> promise) {
        Promise<Void> vp = ImmediateEventExecutor.INSTANCE.newPromise();
        vp.addListener(f -> {
            if (f.isSuccess()) {
                final Offset headOffset = headLocation;
                final Offset tailOffset = tailLocation;
                final Offset currentOffset = getCurrentOffset();
                final SyncResponse response = SyncResponse.newBuilder()
                        .setHeadOffset(MessageOffset.newBuilder()
                                .setEpoch(headOffset.getEpoch())
                                .setIndex(headOffset.getIndex()).build())
                        .setTailOffset(MessageOffset.newBuilder()
                                .setEpoch(tailOffset.getEpoch())
                                .setIndex(tailOffset.getIndex()).build())
                        .setCurrentOffset(MessageOffset.newBuilder()
                                .setEpoch(currentOffset.getEpoch())
                                .setIndex(currentOffset.getIndex()).build())
                        .build();
                promise.trySuccess(response);
            } else {
                promise.tryFailure(f.cause());
            }
        });
        return vp;
    }

    /**
     * Determines the appropriate {@code Offset} based on the provided reference {@code Offset}.
     * If the provided {@code Offset} is {@code null}, the method returns the tail location.
     * Otherwise, it compares the provided {@code Offset} with the head location and returns
     * the greater of the two.
     *
     * @param offset the reference {@code Offset} to be compared or used for location determination
     * @return the appropriate {@code Offset} based on the logic; either the provided {@code Offset},
     *         the head location, or the tail location if the input is {@code null}
     */
    private Offset locate(Offset offset) {
        if (offset == null) {
            return tailLocation;
        }
        if (offset.after(headLocation)) {
            return offset;
        }
        return headLocation;
    }

    /**
     * Updates the last subscribe time to the current system time in milliseconds.
     * <p>
     * This method sets the field {@code lastSubscribeTimeMillis} to the value
     * returned by {@link System#currentTimeMillis()}. It is typically called when
     * a subscribe operation is performed to record the timestamp of the last subscription event.
     */
    private void setLastSubscribeTimeMs() {
        this.lastSubscribeTimeMillis = System.currentTimeMillis();
    }

    /**
     * Synchronizes data from the target location, leveraging the provided client channel
     * and offset. Upon successful synchronization, the location is initialized based
     * on the retrieved {@link SyncResponse}.
     *
     * @param clientChannel the channel representing the client connection to utilize for synchronization
     * @param offset the offset from which to start synchronization
     * @param timeoutMs the duration (in milliseconds) to allow for the synchronization process
     * @param promise the promise to be fulfilled upon successful completion of the synchronization
     */
    @Override
    protected void doSyncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs, Promise<SyncResponse> promise) {
        promise.addListener(future -> {
            if (future.isSuccess()) {
                initLocation((SyncResponse) future.getNow());
            }
        });
        super.doSyncFromTarget(clientChannel, offset, timeoutMs, promise);
    }

    /**
     * Initializes the location based on the provided sync response. This method updates
     * the tail and head locations if the new offsets are greater than the current ones.
     *
     * @param response the sync response containing the head and tail offsets
     */
    private void initLocation(SyncResponse response) {
        if (response == null) {
            return;
        }

        MessageOffset tailOffset = response.getTailOffset();
        Offset thetailOffset = new Offset(tailOffset.getEpoch(), tailOffset.getIndex());
        if (thetailOffset.after(tailLocation)) {
            tailLocation = thetailOffset;
        }

        MessageOffset headOffset = response.getHeadOffset();
        Offset theheadOffset = new Offset(headOffset.getEpoch(), headOffset.getIndex());
        if (thetailOffset.after(headLocation)) {
            headLocation = theheadOffset;
        }
    }

    /**
     * Returns the last time, in milliseconds, when a subscription was made.
     *
     * @return the last subscription time in milliseconds
     */
    public long getLastSubscribeTimeMillis() {
        return lastSubscribeTimeMillis;
    }

    /**
     * Invoked when a new record is appended to a ledger. This method updates the position
     * of the tail location if the appended record is located after the current tail.
     *
     * @param ledgerId    the unique identifier of the ledger where the new record was appended
     * @param recordCount the number of records that were appended
     * @param lastOffset  the offset of the last appended record
     */
    @Override
    protected void onAppendTrigger(int ledgerId, int recordCount, Offset lastOffset) {
        super.onAppendTrigger(ledgerId, recordCount, lastOffset);
        if (lastOffset.after(tailLocation)) {
            tailLocation = lastOffset;
        }
    }

    /**
     * Handles the event when a release trigger is fired.
     * This method updates the headLocation if the new head offset is after the current headLocation.
     *
     * @param ledgerId the identifier of the ledger.
     * @param oldHead the previous head offset.
     * @param newHead the new head offset.
     */
    @Override
    protected void onReleaseTrigger(int ledgerId, Offset oldHead, Offset newHead) {
        super.onReleaseTrigger(ledgerId, oldHead, newHead);
        if (newHead.after(headLocation)) {
            headLocation = newHead;
        }
    }

    /**
     * Handles the incoming push message by updating the count of total dispatched messages.
     *
     * @param count the number of messages being pushed.
     */
    @Override
    protected void onPushMessage(int count) {
        super.onPushMessage(count);
        totalDispatchedMessages.add(count);
    }

    /**
     * Handles synchronization messages specific to the ProxyLog class by updating
     * the total count of dispatched messages.
     *
     * @param count The number of messages to be synchronized.
     */
    @Override
    protected void onSyncMessage(int count) {
        super.onSyncMessage(count);
        totalDispatchedMessages.add(count);
    }

    /**
     * Returns the total number of messages dispatched so far.
     *
     * @return total number of dispatched messages as a long value.
     */
    public long getTotalDispatchedMessages() {
        return totalDispatchedMessages.longValue();
    }
}
