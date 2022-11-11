package org.shallow.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.client.consumer.pull.PullResult;
import org.shallow.client.consumer.push.Subscription;
import org.shallow.servlet.DefaultEntryPullDispatcher;
import org.shallow.servlet.PullDispatchProcessor;
import org.shallow.servlet.DefaultEntryPushDispatcher;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.servlet.PushDispatchProcessor;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicReference;

import static org.shallow.remote.util.ByteBufUtil.release;
import static org.shallow.remote.util.NetworkUtil.newEventExecutorGroup;

@ThreadSafe
public class Ledger {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Ledger.class);

    private final String topic;
    private final int partition;
    private final int ledgerId;
    private int epoch;
    private final Storage storage;
    private final EventExecutor storageExecutor;
    private final PullDispatchProcessor entryPullHandler;
    private final PushDispatchProcessor entryPushHandler;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

    enum State {
        LATENT, STARTED, CLOSED
    }

    public Ledger(BrokerConfig config, String topic, int partition, int ledgerId, int epoch) {
        this.topic = topic;
        this.partition = partition;
        this.ledgerId = ledgerId;
        this.epoch = epoch;
        this.storageExecutor = newEventExecutorGroup(config.getMessageStorageHandleThreadLimit(), "ledger-storage").next();
        this.storage = new Storage(storageExecutor, ledgerId, config, epoch, new MessageTrigger());
        this.entryPullHandler = new DefaultEntryPullDispatcher(config);
        this.entryPushHandler = new DefaultEntryPushDispatcher(ledgerId, config, storage);
    }

    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new UnsupportedOperationException(String.format("Ledger<%d> was started", ledgerId));
        }
    }

    public void subscribe(Channel channel, String topic, String queue, short version, int epoch, long index, Promise<Subscription> promise) {
        Offset offset = Offset.of(epoch, index);
        if (storageExecutor.inEventLoop()) {
            doSubscribe(channel, topic, queue, version, offset, promise);
        } else {
            try {
                storageExecutor.execute(() -> doSubscribe(channel, topic, queue, version, offset, promise));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to subscribe, topic={} partition={} queue={} ledgerId={} epoch={} index={}. error cause:{}",
                            topic, partition, queue, ledgerId, epoch, index, t);
                }
                promise.tryFailure(t);
            }
        }
    }

    private void doSubscribe(Channel channel, String topic, String queue, short version, Offset offset, Promise<Subscription> promise) {
        try {
            Offset theOffset;
            if (offset == null) {
                Segment segment = storage.tailSegment();
                theOffset = segment.tailOffset();
            } else {
               theOffset = offset;
            }

            Promise<Subscription> subscribePromise = storageExecutor.newPromise();
            subscribePromise.addListener((GenericFutureListener<Future<Subscription>>) future -> {
                if (future.isSuccess()) {
                    Subscription subscription = future.get();
                    subscription.setLedger(ledgerId);

                    promise.trySuccess(subscription);
                } else {
                    promise.tryFailure(future.cause());
                }
            });
            entryPushHandler.subscribe(channel, topic, queue, theOffset, version, subscribePromise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to subscribe, channel<{}> ledgerId={} topic={} queue={} offset={}", channel.toString(), ledgerId, topic, queue, offset);
            }
            promise.tryFailure(t);
        }
    }

    public void clean(Channel channel, String topic, String queue, Promise<Void> promise) {
        if (storageExecutor.inEventLoop()) {
            doClean(channel, topic, queue, promise);
        } else {
            try {
                storageExecutor.execute(() -> doClean(channel, topic, queue, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
    }

    private void doClean(Channel channel, String topic, String queue, Promise<Void> promise) {
        try {
            entryPushHandler.clean(channel, topic, queue, promise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to clear subscribe, channel<{}> ledgerId={} topic={} queue={}", channel.toString(), ledgerId, topic, queue);
            }
            promise.tryFailure(t);
        }
    }

    public void append(String queue, short version, ByteBuf payload, Promise<Offset> promise) {
        if (storageExecutor.inEventLoop()) {
            doAppend(topic, queue, version, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(topic, queue, version, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
            }
        }
    }

    private void doAppend(String topic, String queue, short version,  ByteBuf payload, Promise<Offset> promise) {
        try {
            storage.append(topic, queue, version, payload, promise);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
           release(payload);
        }
    }

    public void pull(int requestId, Channel channel, String topic, String queue, short version, int epoch, long index, int limit, Promise<PullResult> promise) {
        if (storageExecutor.inEventLoop()) {
            doPull(requestId, channel, topic, queue, version, epoch, index, limit, promise);
        } else {
            try {
                storageExecutor.execute(() -> doPull(requestId, channel, topic, queue, version, epoch, index, limit, promise));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to pull message, topic={} partition={} queue={} version={} ledgerId={} epoch={} index={} requestId={}. error cause:{}",
                            topic, partition, queue, version, ledgerId, epoch, index, requestId, t);

                }
                promise.tryFailure(t);
            }
        }
    }

    private void doPull(int requestId, Channel channel, String topic, String queue, short version, int epoch, long index, int limit, Promise<PullResult> promise) {
        Offset offset = new Offset(epoch, index);
        try {
            entryPullHandler.register(requestId, channel);
            storage.read(requestId, topic, queue, version, offset, limit, promise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to doPull message, topic={} partition={} queue={} version={} ledgerId={} epoch={} index={} requestId={}. error cause:{}",
                        topic, partition, queue, version, ledgerId, epoch, index, requestId, t);

            }
            promise.tryFailure(t);
        }
    }

    public void clearChannel(Channel channel) {
        entryPushHandler.clearChannel(channel);
    }

    @SuppressWarnings("unused")
    public void onTriggerAppend(int limit, Offset offset) {
        entryPushHandler.handle(topic);
    }

    public void onTriggerPull(int requestId, String queue, short version, int ledgerId, int limit, Offset  offset, ByteBuf buf) {
        entryPullHandler.dispatch(requestId, topic, queue, version, ledgerId, limit, offset, buf);
    }

    private class MessageTrigger implements LedgerTrigger {
        @Override
        public void onAppend(int limit, Offset tail) {
            onTriggerAppend(limit, tail);
        }

        @Override
        public void onPull(int requestId, String queue, short version, int ledgerId, int limit, Offset head, ByteBuf buf) {
            onTriggerPull(requestId, queue, version, ledgerId, limit, head, buf);
        }
    }

    public String getTopic() {
        return topic;
    }

    @SuppressWarnings("unused")
    public int getPartition() {
        return partition;
    }

    public int getLedgerId() {
        return ledgerId;
    }

    @SuppressWarnings("unused")
    public int getEpoch() {
        return epoch;
    }

    @SuppressWarnings("unused")
    public void epoch(int epoch) {
        this.epoch = epoch;
    }

    public State getState() {
        return state.get();
    }

    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            storageExecutor.shutdownGracefully();

            storage.close();

            entryPullHandler.shutdownGracefully();
            entryPushHandler.shutdownGracefully();

            if (logger.isWarnEnabled()) {
                logger.warn("Close ledger<{}> successfully, topic={} partition={}", ledgerId, topic, partition);
            }
        }
    }
}
