package org.ostara.core.inner;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientListener;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.management.Manager;
import org.ostara.remote.proto.client.SyncMessageSignal;

import java.util.concurrent.Semaphore;

public class InnerClientListener implements ClientListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InnerClientListener.class);

    private final CoreConfig config;
    private final Manager manager;
    private final FastThreadLocal<Semaphore> threadSemaphore = new FastThreadLocal<>() {
        @Override
        protected Semaphore initialValue() throws Exception {
            return new Semaphore(100);
        }
    };

    public InnerClientListener(CoreConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
    }

    @Override
    public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
        Semaphore semaphore = threadSemaphore.get();
        semaphore.acquireUninterruptibly();
        try {
            int ledger = signal.getLedger();
            int count = signal.getCount();
            Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener(future -> semaphore.release());

        } catch (Throwable t) {
            semaphore.release();
            logger.error(t.getMessage(), t);
        }
    }
}
