package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.consumer.pull.PullResult;
import org.shallow.consumer.push.Subscription;
import org.shallow.handle.EntryPullHandler;
import org.shallow.handle.EntryPushHandler;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import static org.shallow.util.ByteBufUtil.release;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

@ThreadSafe
public class Ledger {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Ledger.class);

    private final String topic;
    private final int partition;
    private final int ledgerId;
    private int epoch;
    private final BrokerConfig config;
    private final Storage storage;
    private final EventExecutor storageExecutor;
    private final EntryPullHandler entryPullHandler;
    private final EntryPushHandler entryPushHandler;

    public Ledger(BrokerConfig config, String topic, int partition, int ledgerId, int epoch) {
        this.topic = topic;
        this.partition = partition;
        this.ledgerId = ledgerId;
        this.config = config;
        this.epoch = epoch;
        this.storageExecutor = newEventExecutorGroup(1, "ledger-storage").next();
        this.storage = new Storage(storageExecutor, ledgerId, config, epoch, new MessageTrigger());
        this.entryPullHandler = new EntryPullHandler(config);
        this.entryPushHandler = new EntryPushHandler(config);
    }

    public String getTopic() {
        return topic;
    }

    public void subscribe(String queue, int epoch, long index, Promise<Subscription> promise) {
        Offset offset = Offset.of(epoch, index);
        if (storageExecutor.inEventLoop()) {
            doSubscribe(queue, offset, promise);
        } else {
            try {
                storageExecutor.execute(() -> doSubscribe(queue, offset, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
    }

    private void doSubscribe(String queue, Offset offset, Promise<Subscription> promise) {

    }

    public void append(String queue, short version, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (storageExecutor.inEventLoop()) {
            doAppend(queue, version, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(queue, version, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
            }
        }
    }

    private void doAppend(String queue, short version,  ByteBuf payload, Promise<Offset> promise) {
        try {
            storage.append(queue, version, payload, promise);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
           release(payload);
        }
    }

    public void pull(int requestId, Channel channel, String queue, short version, int epoch, long index, int limit, Promise<PullResult> promise) {
        if (storageExecutor.inEventLoop()) {
            doPull(requestId, channel, queue, version, epoch, index, limit, promise);
        } else {
            try {
                storageExecutor.execute(() -> doPull(requestId, channel, queue, version, epoch, index, limit, promise));
            } catch (Throwable t) {
                promise.tryFailure(t);
            }
        }
    }

    private void doPull(int requestId, Channel channel, String queue, short version, int epoch, long index, int limit, Promise<PullResult> promise) {
        Offset offset = new Offset(epoch, index);
        try {
            entryPullHandler.register(requestId, channel);
            storage.read(requestId, queue, version, offset, limit, promise);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void onTriggerAppend(int limit, Offset offset) {
        entryPushHandler.handle();
    }

    public void onTriggerPull(int requestId, String queue, short version, int ledgerId, int limit, Offset  offset, ByteBuf buf) {
        entryPullHandler.handle(requestId, topic, queue, version, ledgerId, limit, offset, buf);
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

    public void close() {
        entryPullHandler.shutdownGracefully();
        storageExecutor.shutdownGracefully();
        storage.close();
    }
}
