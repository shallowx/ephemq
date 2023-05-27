package org.ostara.common.thread;

import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

public class FastEventExecutor extends SingleThreadEventExecutor {

    public FastEventExecutor(ThreadFactory factory) {
        this(null, factory, true, Integer.MAX_VALUE, RejectedExecutionHandlers.reject());
    }

    public FastEventExecutor(EventExecutorGroup parent, ThreadFactory factory, boolean addTaskWakeup,
                             int maxPendingTasks, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, factory, addTaskWakeup, maxPendingTasks, rejectedExecutionHandler);
    }

    public FastEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakeup, int maxPendingTasks, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, addTaskWakeup, maxPendingTasks, rejectedExecutionHandler);
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        if (maxPendingTasks <= 0 || maxPendingTasks > (1 << 30)) {
            return new LinkedBlockingQueue<>();
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
                } catch (Throwable ignored) {}
                updateLastExecutionTime();
            }
        }while (!confirmShutdown());
    }
}
