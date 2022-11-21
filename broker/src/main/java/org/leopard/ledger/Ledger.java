package org.leopard.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Subscription;
import org.leopard.dispatch.DefaultEntryDispatchProcessor;
import org.leopard.dispatch.DispatchProcessor;
import org.leopard.internal.config.ServerConfig;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

import static org.leopard.remote.util.ByteBufUtils.release;
import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;

@ThreadSafe
public class Ledger {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Ledger.class);

    private final String topic;
    private final int partition;
    private final int ledgerId;
    private int epoch;
    private final Storage storage;
    private final EventExecutor storageExecutor;
    private final DispatchProcessor processor;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

    enum State {
        LATENT, STARTED, CLOSED
    }

    public Ledger(ServerConfig config, String topic, int partition, int ledgerId, int epoch) {
        this.topic = topic;
        this.partition = partition;
        this.ledgerId = ledgerId;
        this.epoch = epoch;
        this.storageExecutor = newEventExecutorGroup(config.getMessageStorageHandleThreadLimit(), "ledger-storage").next();
        this.storage = new Storage(storageExecutor, ledgerId, config, epoch, new MessageTrigger());
        this.processor = new DefaultEntryDispatchProcessor(ledgerId, config, storage);
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
            processor.subscribe(channel, topic, queue, theOffset, version, subscribePromise);
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
            processor.clean(channel, topic, queue, promise);
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

    private void doAppend(String topic, String queue, short version, ByteBuf payload, Promise<Offset> promise) {
        try {
            storage.append(topic, queue, version, payload, promise);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
            release(payload);
        }
    }

    public void clearChannel(Channel channel) {
        processor.clearChannel(channel);
    }

    @SuppressWarnings("unused")
    public void onTriggerAppend(int limit, Offset offset) {
        processor.handleRequest(topic);
    }

    private class MessageTrigger implements LedgerTrigger {
        @Override
        public void onAppend(int limit, Offset tail) {
            onTriggerAppend(limit, tail);
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
            processor.shutdownGracefully();

            if (logger.isWarnEnabled()) {
                logger.warn("Close ledger<{}> successfully, topic={} partition={}", ledgerId, topic, partition);
            }
        }
    }
}
