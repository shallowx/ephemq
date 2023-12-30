package org.meteor.proxy.internal;

import io.netty.channel.Channel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.IntList;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.message.Offset;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.listener.TopicListener;
import org.meteor.coordinatior.Coordinator;
import org.meteor.proxy.MeteorProxy;
import org.meteor.remote.proto.MessageOffset;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.ledger.Log;
import java.util.concurrent.atomic.LongAdder;

public class ProxyLog extends Log {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    private volatile long lastSubscribeTimeMs = System.currentTimeMillis();
    private volatile Offset tailLocation = new Offset(0, 0);
    private volatile Offset headLocation = new Offset(0, 0);
    private final LongAdder totalDispatchedMessages = new LongAdder();
    private final ProxyConfig proxyConfiguration;
    public ProxyLog(ProxyServerConfig config, TopicPartition topicPartition, int ledger, int epoch, Coordinator coordinator, TopicConfig topicConfig) {
        super(config, topicPartition, ledger, epoch, coordinator, topicConfig);
        this.proxyConfiguration = config.getProxyConfiguration();
    }

    public void cancelSyncAndCloseIfNotSubscribe(Promise<Boolean> promise) {
        if (storageExecutor.inEventLoop()) {
            doCancelSyncAndCloseIfNotSubscribe(promise);
        } else {
           try {
               storageExecutor.execute(() -> {
                   doCancelSyncAndCloseIfNotSubscribe(promise);
               });
           } catch (Exception e){
               if (logger.isDebugEnabled()) {
                   logger.debug(e.getMessage(), e);
               }
               promise.tryFailure(e);
           }
        }
    }

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
            Promise<CancelSyncResponse> cancelSyncPromise = cancelSync(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMs());
            cancelSyncPromise.addListener(f -> {
                if (f.isSuccess()) {
                    Promise<Boolean> closePromise = storageExecutor.newPromise();
                    closePromise.addListener(future -> {
                        if (future.isSuccess()) {
                            promise.trySuccess((Boolean) future.get());
                            for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
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
                for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
                    listener.onPartitionDestroy(topicPartition, ledger);
                }
            } else {
                promise.tryFailure(f.cause());
            }
        });
        close(closePromise);
    }

    public void syncAndResetSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, IntList markerSet, Promise<Integer> promise) {
      Promise<Integer> ret = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
      if (storageExecutor.inEventLoop()) {
          doSyncAndResetSubscribe(syncChannel, epoch, index, channel, markerSet, promise);
      } else {
          try {
              storageExecutor.execute(() -> {
                  doSyncAndResetSubscribe(syncChannel, epoch, index, channel, markerSet, promise);
              });
          } catch (Exception e){
              if (logger.isDebugEnabled()) {
                  logger.debug(e.getMessage(), e);
              }
              ret.tryFailure(e);
          }
      }
    }

    private void doSyncAndResetSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, IntList markerSet, Promise<Integer> promise) {
        LogState logState = state.get();
        if (!isSynchronizing(logState)) {
            Promise<SyncResponse> syncPromise = syncFromTarget(syncChannel, new Offset(0, 0), proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMs());
            syncPromise.addListener(f -> {
               if (f.isSuccess()) {
                  Offset locate = locate(Offset.of(epoch, index));
                  resetSubscribe(channel, locate, markerSet, promise);
               } else {
                   promise.tryFailure(f.cause());
               }
            });
        } else {
            if (syncFuture != null && !syncFuture.isDone()) {
                syncFuture.addListener(future -> {
                   if (storageExecutor.inEventLoop()) {
                       resetSubscribe(channel, locate(Offset.of(epoch, index)), markerSet, promise);
                   } else {
                       try {
                           storageExecutor.execute(() -> {
                               resetSubscribe(channel, locate(Offset.of(epoch, index)), markerSet, promise);
                           });
                       }catch (Exception e){
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

    public void syncAndChunkSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Promise<SyncResponse> ret = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (storageExecutor.inEventLoop()) {
            doSyncAndChunkSubscribe(syncChannel, epoch, index, channel, promise);
        } else {
            try {
                storageExecutor.execute(() -> {
                    doSyncAndChunkSubscribe(syncChannel, epoch, index, channel, promise);
                });
            } catch (Exception e){
                if (logger.isDebugEnabled()) {
                    logger.debug(e.getMessage(), e);
                }
                ret.tryFailure(e);
            }
        }
    }

    private void doSyncAndChunkSubscribe(ClientChannel syncChannel, int epoch, long index, Channel channel, Promise<SyncResponse> promise) {
        Promise<Void> vp = getVoidPromise(promise);
        LogState logState = state.get();
        if (!isSynchronizing(logState)) {
            Promise<SyncResponse> syncPromise = syncFromTarget(syncChannel, new Offset(0, 0), proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMs());
            syncPromise.addListener(future -> {
                if (future.isSuccess()) {
                    attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                } else {
                    promise.tryFailure(future.cause());
                }
            });
        } else {
            if (syncFuture != null && !syncFuture.isDone()) {
                syncFuture.addListener(future -> {
                    if (storageExecutor.inEventLoop()) {
                        attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                    } else {
                        try {
                            storageExecutor.execute(() -> {
                                attachSynchronize(channel, locate(Offset.of(epoch, index)), vp);
                            });
                        }catch (Exception e){
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

    private Offset locate(Offset offset) {
        if (offset == null) {
            return tailLocation;
        }
        if (offset.after(headLocation)) {
            return offset;
        }
        return headLocation;
    }

    private void setLastSubscribeTimeMs() {
        this.lastSubscribeTimeMs = System.currentTimeMillis();
    }

    @Override
    protected void doSyncFromTarget(ClientChannel clientChannel, Offset offset, int timeoutMs, Promise<SyncResponse> promise) {
        promise.addListener(future -> {
           if (future.isSuccess()) {
               initLocation((SyncResponse)future.getNow());
           }
        });
        super.doSyncFromTarget(clientChannel, offset, timeoutMs, promise);
    }

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

    public long getLastSubscribeTimeMs() {
        return lastSubscribeTimeMs;
    }

    @Override
    protected void onAppendTrigger(int ledgerId, int recordCount, Offset lastOffset) {
        super.onAppendTrigger(ledgerId, recordCount, lastOffset);
        if (lastOffset.after(tailLocation)) {
            tailLocation = lastOffset;
        }
    }

    @Override
    protected void onReleaseTrigger(int ledgerId, Offset oldHead, Offset newHead) {
        super.onReleaseTrigger(ledgerId, oldHead, newHead);
        if (newHead.after(headLocation)) {
            headLocation = newHead;
        }
    }

    @Override
    protected void onPushMessage(int count) {
        super.onPushMessage(count);
        totalDispatchedMessages.add(count);
    }

    @Override
    protected void onSyncMessage(int count) {
        super.onSyncMessage(count);
        totalDispatchedMessages.add(count);
    }

    public long getTotalDispatchedMessages() {
        return totalDispatchedMessages.longValue();
    }
}
