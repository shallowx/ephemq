package org.ostara.management;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ostara.client.internal.ClientChannel;
import org.ostara.common.Offset;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.thread.FastEventExecutor;
import org.ostara.core.CoreConfig;
import org.ostara.ledger.LogManager;
import org.ostara.remote.RemoteException;
import org.ostara.remote.proto.MessageOffset;
import org.ostara.remote.proto.server.CancelSyncRequest;
import org.ostara.remote.proto.server.CancelSyncResponse;
import org.ostara.remote.proto.server.SyncRequest;
import org.ostara.remote.proto.server.SyncResponse;
import org.ostara.ledger.Log;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ParticipantManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ParticipantManager.class);
    private final CoreConfig config;
    private final Manager manager;
    private final Map<Integer, ClientChannel> ledgerChannels = new ConcurrentHashMap<>();
    private EventExecutor fetchExecutor;

    public ParticipantManager(CoreConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
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
        Log log = manager.getLogManager().getLog(ledger);
        if (log == null) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d not found", ledger)));
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
    }

    public Promise<SyncResponse> syncLeader(TopicPartition topicPartition, int ledger, ClientChannel channel,
                                            int epoch, long index, int timeoutMs, Promise<SyncResponse> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }

        try {
            Log log = manager.getLogManager().getLog(ledger);
            if (log == null) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("Ledger %d not found", ledger)));
                return promise;
            }
            logger.info("Synchronize data of {} from {} with epoch: {}, index:{}", ledger, channel, epoch, index);
            SyncRequest request = SyncRequest.newBuilder()
                    .setEpoch(epoch)
                    .setIndex(index)
                    .setTopic(topicPartition.getTopic())
                    .setLedger(ledger)
                    .build();

            channel.invoker().syncMessage(timeoutMs, promise, request);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getLocalizedMessage(), e);
        }
        return promise;
    }

    public Promise<CancelSyncResponse> unSyncLedger(TopicPartition topicPartition, int ledger, ClientChannel channel, int timeoutMs, Promise<CancelSyncResponse> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }

        try {
            CancelSyncRequest request = CancelSyncRequest.newBuilder()
                    .setLedger(ledger)
                    .setTopic(topicPartition.getTopic())
                    .build();

            channel.invoker().cancelSyncMessage(timeoutMs, promise, request);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error(e.getLocalizedMessage(), e);
        }

        return promise;
    }

    public void unSubscribeLedger(int ledger, Channel channel, Promise<Void> promise) {
        LogManager logManager = manager.getLogManager();
        Log log = logManager.getLog(ledger);
        if (log == null) {
            promise.trySuccess(null);
            return;
        }
        log.detachSynchronize(channel, promise);
    }

    public Promise<Void> stopChunkDispatch(int ledger, Promise<Void> promise) {
        if (promise == null) {
            promise = ImmediateEventExecutor.INSTANCE.newPromise();
        }
        try {
            Log log = manager.getLogManager().getLog(ledger);
            if (log == null) {
                promise.trySuccess(null);
                return promise;
            }
            log.detachAllSynchronize(promise);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
        return promise;
    }
}
