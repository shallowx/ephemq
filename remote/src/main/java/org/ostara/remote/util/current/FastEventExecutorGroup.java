package org.ostara.remote.util.current;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

public class FastEventExecutorGroup extends MultithreadEventExecutorGroup {

    public FastEventExecutorGroup(int nThreads) {
        this(nThreads, null);
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, true);
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory threadFactory, boolean wakesUp) {
        this(nThreads, threadFactory, wakesUp, Integer.MAX_VALUE);
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory threadFactory, boolean wakesUp, int maxPendingTasks) {
        this(nThreads, threadFactory, wakesUp, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    public FastEventExecutorGroup(int nThreads, ThreadFactory threadFactory, boolean wakesUp, int maxPendingTasks,
                                  RejectedExecutionHandler rejectedExecutionHandler) {
        super(nThreads, threadFactory, wakesUp, maxPendingTasks, rejectedExecutionHandler);
    }

    @Override
    protected EventExecutor newChild(Executor executor, Object... args) throws Exception {
        return new FastEventExecutor(this, executor, (Boolean) args[0], (Integer) args[1],
                (RejectedExecutionHandler) args[2]);
    }
}
