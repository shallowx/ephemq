package org.meteor.ledger;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicPartition;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.remote.exception.RemotingException;
import org.meteor.remote.proto.MessageOffset;
import org.meteor.remote.proto.server.CancelSyncRequest;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncRequest;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.support.Manager;

/**
 * The ParticipantSupport class provides methods to manage participant subscriptions
 * and synchronization with the state log within a distributed system.
 * It is responsible for starting participant support, subscribing to ledgers,
 * synchronizing with the leader, unsynchronizing from ledgers, and handling shutdowns.
 */
public class ParticipantSupport {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ParticipantSupport.class);
    /**
     * The Manager instance responsible for handling various server components and their interactions
     * as part of the ParticipantSupport class. This includes starting and shutting down operations,
     * managing topics, clusters, logging, connections, and event executors.
     */
    private final Manager manager;
    /**
     * The executor responsible for handling event execution within
     * the {@code ParticipantSupport} class.
     */
    private EventExecutor executor;

    /**
     * Initializes a new instance of the ParticipantSupport class with the provided manager.
     *
     * @param manager The Manager instance to be used for managing various server components and interactions within the ParticipantSupport context.
     */
    public ParticipantSupport(Manager manager) {
        this.manager = manager;
    }

    /**
     * Initializes the FastEventExecutor for handling replica fetch operations.
     *
     * @throws Exception if initialization of the executor fails.
     */
    public void start() throws Exception {
        executor = new FastEventExecutor(new DefaultThreadFactory("replica_fetch"));
    }

    /**
     * Subscribes a ledger to synchronize the state of offsets with a client channel.
     *
     * @param ledger  The identifier of the ledger to subscribe to.
     * @param epoch   The epoch number from which to start the synchronization.
     * @param index   The index within the epoch from which to start the synchronization.
     * @param channel The client communication channel to be used for the synchronization.
     * @param promise The promise to be fulfilled with the result of the synchronization.
     */
    public void subscribeLedger(int ledger, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Log log = manager.getLogHandler().getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(
                    RemotingException.Failure.PROCESS_EXCEPTION, STR."The ledger[\{ledger}] not found"));
            return;
        }

        Promise<Void> attachPromise = ImmediateEventExecutor.INSTANCE.newPromise();
        attachPromise.addListener(future -> {
            if (future.isSuccess()) {
                Offset headOffset = log.getHeadOffset();
                Offset tailOffset = log.getTailOffset();
                Offset currentOffset = log.getCurrentOffset();

                SyncResponse response = SyncResponse.newBuilder()
                        .setHeadOffset(MessageOffset.newBuilder()
                                .setEpoch(headOffset.getEpoch())
                                .setIndex(headOffset.getIndex())
                                .build())
                        .setTailOffset(MessageOffset.newBuilder()
                                .setEpoch(tailOffset.getEpoch())
                                .setIndex(tailOffset.getIndex())
                                .build())
                        .setCurrentOffset(MessageOffset.newBuilder()
                                .setEpoch(currentOffset.getEpoch())
                                .setIndex(currentOffset.getIndex())
                                .build())
                        .build();
                promise.trySuccess(response);
            } else {
                promise.tryFailure(future.cause());
            }
        });
        log.attachSynchronize(channel, Offset.of(epoch, index), attachPromise);
    }

    /**
     * Synchronizes the leader state for the given topic partition and ledger.
     *
     * @param topicPartition The topic partition associated with the ledger.
     * @param ledger         The ledger identifier to synchronize.
     * @param channel        The channel through which the synchronization request will be sent.
     * @param epoch          The epoch number associated with the ledger state.
     * @param index          The index position in the ledger to start synchronization from.
     * @param timeoutMs      Timeout in milliseconds for the synchronization request.
     * @param promise        A promise that will be completed with the result of the synchronization process.
     */
    public void syncLeader(TopicPartition topicPartition, int ledger, ClientChannel channel,
                           int epoch, long index, int timeoutMs, Promise<SyncResponse> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }
        try {
            Log log = manager.getLogHandler().getLog(ledger);
            if (log == null) {
                promise.tryFailure(RemotingException.of(
                        RemotingException.Failure.PROCESS_EXCEPTION, STR."The ledger[\{ledger}] not found"));
                return;
            }
            if (logger.isInfoEnabled()) {
                logger.info("Synchronize data of ledger[{}] from channel[{}] with epoch[{}] and index[{}]", ledger, channel, epoch, index);
            }
            SyncRequest request = SyncRequest.newBuilder()
                    .setEpoch(epoch)
                    .setIndex(index)
                    .setTopic(topicPartition.topic())
                    .setLedger(ledger)
                    .build();

            channel.invoker().syncMessage(timeoutMs, promise, request);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Unsynchronizes the specified ledger for the given topic partition through the provided client channel.
     *
     * @param topicPartition the topic partition from which the ledger will be unsynchronized
     * @param ledger the ledger identifier to be unsynchronized
     * @param channel the client channel used to communicate the unsynchronization request
     * @param timeoutMs the timeout in milliseconds for the request
     * @param promise the promise to be fulfilled with the response of the unsynchronization operation
     */
    public void unSyncLedger(TopicPartition topicPartition, int ledger, ClientChannel channel, int timeoutMs, Promise<CancelSyncResponse> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }
        try {
            CancelSyncRequest request = CancelSyncRequest.newBuilder()
                    .setLedger(ledger)
                    .setTopic(topicPartition.topic())
                    .build();

            channel.invoker().cancelSyncMessage(timeoutMs, promise, request);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Unsubscribes from the specified ledger.
     *
     * @param ledger The ledger to be unsubscribed.
     * @param channel The channel associated with the ledger subscription.
     * @param promise A promise to be completed upon the completion of the unsubscription.
     */
    public void unSubscribeLedger(int ledger, Channel channel, Promise<Void> promise) {
        LogHandler handler = manager.getLogHandler();
        Log log = handler.getLog(ledger);
        if (log == null) {
            promise.trySuccess(null);
            return;
        }
        log.subscribeSynchronize(channel, promise);
    }

    /**
     * Stops the chunk dispatching for a given ledger.
     *
     * @param ledger the ledger for which chunk dispatching is to be stopped.
     * @param promise a Promise to be completed once the operation is finished.
     * @return a Promise that completes when the chunk dispatch is stopped. If the provided promise is null, a new promise will be created and returned.
     */
    public Promise<Void> stopChunkDispatch(int ledger, Promise<Void> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }
        try {
            Log log = manager.getLogHandler().getLog(ledger);
            if (log == null) {
                promise.trySuccess(null);
                return promise;
            }
            log.cancelAllSynchronize(promise);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
        return promise;
    }


    /**
     * Shuts down the executor service gracefully, preventing the submission of new tasks and
     * attempting to complete previously submitted tasks.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    public void shutdown() throws Exception {
        if (executor != null) {
            executor.shutdownGracefully();
        }
    }
}
