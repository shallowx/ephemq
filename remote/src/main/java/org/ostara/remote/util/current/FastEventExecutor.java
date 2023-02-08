package org.ostara.remote.util.current;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.handle.ConnectDuplexHandler;

public class FastEventExecutor extends SingleThreadEventExecutor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConnectDuplexHandler.class);

    public static final int MAX_CAPACITY = 1 << 30;

    public FastEventExecutor() {
        this(new DefaultThreadFactory(FastEventExecutor.class));
    }

    public FastEventExecutor(ThreadFactory threadFactory) {
        this(null, threadFactory, true, Integer.MAX_VALUE, RejectedExecutionHandlers.reject());
    }

    public FastEventExecutor(Executor executor) {
        this(null, executor, true, Integer.MAX_VALUE, RejectedExecutionHandlers.reject());
    }

    public FastEventExecutor(EventExecutorGroup parent, ThreadFactory threadFactory, boolean wakesUp,
                             int maxPendingTasks, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, threadFactory, wakesUp, maxPendingTasks, rejectedExecutionHandler);
    }

    public FastEventExecutor(EventExecutorGroup parent, Executor executor, boolean wakesUp, int maxPendingTasks,
                             RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, wakesUp, maxPendingTasks, rejectedExecutionHandler);
    }


    public FastEventExecutor(EventExecutorGroup parent, Executor executor, boolean wakesUp, Queue<Runnable> taskQueue,
                             RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, wakesUp, taskQueue, rejectedExecutionHandler);
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        if (maxPendingTasks <= 0 || maxPendingTasks > MAX_CAPACITY) {
            return new LinkedBlockingDeque<>();
        }

        return new MpscBlockingConsumerArrayQueue<>(maxPendingTasks);
    }

    @Override
    protected void run() {
        do {
            Runnable task = takeTask();

            if (task != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    logger.error("Unexpected exception:" + Thread.currentThread().getName(), t);
                }
            }
        } while (!confirmShutdown());
    }
}
