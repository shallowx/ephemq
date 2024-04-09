package org.meteor.support;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.meteor.client.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicPartition;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogHandler;
import org.meteor.remote.exception.RemotingException;
import org.meteor.remote.proto.MessageOffset;
import org.meteor.remote.proto.server.CancelSyncRequest;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncRequest;
import org.meteor.remote.proto.server.SyncResponse;

public class ParticipantCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ParticipantCoordinator.class);
    private final Coordinator coordinator;
    private EventExecutor fetchExecutor;

    public ParticipantCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void start() throws Exception {
        fetchExecutor = new FastEventExecutor(new DefaultThreadFactory("replica_fetch"));
    }

    public void shutdown() throws Exception {
        if (fetchExecutor != null) {
            fetchExecutor.shutdownGracefully();
        }
    }

    public void subscribeLedger(int ledger, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Log log = coordinator.getLogCoordinator().getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemotingException.of(
                    RemotingException.Failure.PROCESS_EXCEPTION, String.format("The ledger[%d] not found", ledger)));
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

    public void syncLeader(TopicPartition topicPartition, int ledger, ClientChannel channel,
                           int epoch, long index, int timeoutMs, Promise<SyncResponse> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }

        try {
            Log log = coordinator.getLogCoordinator().getLog(ledger);
            if (log == null) {
                promise.tryFailure(RemotingException.of(
                        RemotingException.Failure.PROCESS_EXCEPTION,
                        String.format("The ledger [%d] not found", ledger)));
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

    public void unSubscribeLedger(int ledger, Channel channel, Promise<Void> promise) {
        LogHandler logCoordinator = coordinator.getLogCoordinator();
        Log log = logCoordinator.getLog(ledger);
        if (log == null) {
            promise.trySuccess(null);
            return;
        }
        log.subscribeSynchronize(channel, promise);
    }

    public Promise<Void> stopChunkDispatch(int ledger, Promise<Void> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }
        try {
            Log log = coordinator.getLogCoordinator().getLog(ledger);
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
}
