package org.meteor.common.thread;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

public class FastEventExecutorGroup extends MultithreadEventExecutorGroup {

    public FastEventExecutorGroup(int nThreads, ThreadFactory factory) {
        this(nThreads, factory, true);
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup) {
        this(nThreads, factory, addTaskWakeup, Integer.MAX_VALUE);
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup, int maxPendingTasks) {
        this(nThreads, factory, addTaskWakeup, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup, int maxPendingTasks, RejectedExecutionHandler handler) {
        super(nThreads, factory, addTaskWakeup, maxPendingTasks, handler);
    }

    @Override
    protected EventExecutor newChild(Executor executor, Object... args) throws Exception {
        return new FastEventExecutor(this, executor,
                (Boolean) args[0], (Integer) args[1], (RejectedExecutionHandler) args[2]);
    }
}
