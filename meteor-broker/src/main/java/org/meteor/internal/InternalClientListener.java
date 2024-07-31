package org.meteor.internal;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.Semaphore;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.support.Manager;

public class InternalClientListener implements CombineListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalClientListener.class);
    private final Manager manager;
    private final int semaphore;
    private final FastThreadLocal<Semaphore> threadSemaphore = new FastThreadLocal<>() {
        @Override
        protected Semaphore initialValue() throws Exception {
            return new Semaphore(semaphore);
        }
    };

    public InternalClientListener(Manager manager, int semaphore) {
        this.manager = manager;
        this.semaphore = semaphore;
    }

    @Override
    public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
        Semaphore semaphore = threadSemaphore.get();
        semaphore.acquireUninterruptibly();
        try {
            int ledger = signal.getLedger();
            int count = signal.getCount();
            Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener(future -> {
                semaphore.release();
                if (!future.isSuccess()) {
                    if (logger.isErrorEnabled()) {
                        logger.error(STR."Channel[\{channel.toString()}] sync message error");
                    }
                }
            });
            manager.getLogHandler().saveSyncData(channel.channel(), ledger, count, data, promise);
        } catch (Throwable t) {
            semaphore.release();
            logger.error(t.getMessage(), t);
        }
    }
}
