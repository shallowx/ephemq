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

/**
 * The InternalClientListener class implements the CombineListener interface to handle synchronization messages
 * from a client channel. It utilizes a semaphore for thread synchronization and manages log handling through
 * a manager instance.
 */
public class InternalClientListener implements CombineListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalClientListener.class);
    /**
     * A manager instance that handles various operational aspects required by the InternalClientListener.
     * <p>
     * This Manager instance is responsible for:
     * - Starting and shutting down relevant services.
     * - Providing support for topic handling, cluster management, logging, connections, and metrics listening.
     * - Managing executor groups for command handling, message storage, message dispatching, and auxiliary tasks.
     * - Providing access to API listeners and an internal client.
     */
    private final Manager manager;
    /**
     * The semaphore value used to initialize the semaphore for thread synchronization.
     */
    private final int semaphore;
    /**
     * A thread-local variable that holds an instance of Semaphore for each thread.
     * The semaphore is used for thread synchronization within the InternalClientListener class.
     * It is initialized with the value of the semaphore field from the containing InternalClientListener class.
     */
    private final FastThreadLocal<Semaphore> threadSemaphore = new FastThreadLocal<>() {
        @Override
        protected Semaphore initialValue() throws Exception {
            return new Semaphore(semaphore);
        }
    };

    /**
     * Constructs an InternalClientListener instance to handle synchronization messages from a client channel.
     * Uses a semaphore for thread synchronization and a manager instance for log handling.
     *
     * @param manager The manager instance used for log handling and other operations
     * @param semaphore The semaphore value used for initializing the semaphore for thread synchronization
     */
    public InternalClientListener(Manager manager, int semaphore) {
        this.manager = manager;
        this.semaphore = semaphore;
    }

    /**
     * Handles the synchronization message from a client channel.
     *
     * @param channel the client channel from which the synchronization message is received
     * @param signal  the synchronization message signal containing metadata such as ledger and count
     * @param data    the actual data buffer associated with the synchronization message
     */
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
                        logger.error("sync message error - channel[{}]", channel.toString());
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
