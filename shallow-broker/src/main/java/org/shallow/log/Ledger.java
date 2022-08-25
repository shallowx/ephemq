package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.consumer.Subscription;
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

    public Ledger(BrokerConfig config, String topic, int partition, int ledgerId, int epoch) {
        this.topic = topic;
        this.partition = partition;
        this.ledgerId = ledgerId;
        this.config = config;
        this.epoch = epoch;
        this.storageExecutor = newEventExecutorGroup(1, "ledger-storage").next();
        this.storage = new Storage(storageExecutor, ledgerId, config, epoch, new MessageTrigger());
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

    public void append(String queue, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (storageExecutor.inEventLoop()) {
            doAppend(queue, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(queue, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
            }
        }
    }

    private void doAppend(String queue, ByteBuf payload, Promise<Offset> promise) {
        try {
            storage.append(queue, payload, promise);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
           release(payload);
        }
    }

    public int getLedgerId() {
        return ledgerId;
    }

    public void onTriggerAppend(int ledgerId, int limit, Offset offset) {

    }

    private class MessageTrigger implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int limit, Offset tail) {
            onTriggerAppend(ledgerId, limit, tail);
        }
    }
}
