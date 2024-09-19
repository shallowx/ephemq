package org.meteor.common.thread;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * FastEventExecutorGroup is a specialized subclass of MultithreadEventExecutorGroup designed to handle
 * tasks quickly and efficiently by utilizing multiple threads.
 * <p>
 * Multiple constructors allow creation of an instance with various configurations for thread count,
 * thread factory, task wakeup handling, maximum pending tasks, and a rejected execution handler.
 */
public class FastEventExecutorGroup extends MultithreadEventExecutorGroup {

    /**
     * Constructs a new FastEventExecutorGroup with the specified number of threads and thread factory.
     *
     * @param nThreads the number of threads to be used by this executor group.
     * @param factory the ThreadFactory to be used for creating threads.
     */
    public FastEventExecutorGroup(int nThreads, ThreadFactory factory) {
        this(nThreads, factory, true);
    }

    /**
     * Constructs a new FastEventExecutorGroup instance with a specified number of threads,
     * thread factory, and a flag indicating whether to add task wakeup.
     *
     * @param nThreads the number of threads to be used in the executor group
     * @param factory the factory to be used for creating new threads
     * @param addTaskWakeup true if a task wakeup should be added, false otherwise
     */
    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup) {
        this(nThreads, factory, addTaskWakeup, Integer.MAX_VALUE);
    }

    /**
     * Constructs a new FastEventExecutorGroup with the specified configuration.
     *
     * @param nThreads         the number of threads that will be used by this executor group
     * @param factory          the ThreadFactory to use for creating new threads
     * @param addTaskWakeup    whether tasks should wake up the executor when added
     * @param maxPendingTasks  the maximum number of tasks allowed to be pending in the queue
     */
    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup, int maxPendingTasks) {
        this(nThreads, factory, addTaskWakeup, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    /**
     * Constructs a new FastEventExecutorGroup with the specified number of threads, thread factory,
     * task wakeup handling, maximum pending tasks, and rejected execution handler.
     *
     * @param nThreads the number of threads to be used by this executor group.
     * @param factory the ThreadFactory to be used for creating new threads.
     * @param addTaskWakeup true if a task wakeup should be added, false otherwise.
     * @param maxPendingTasks the maximum number of tasks to be held in the pending queue.
     * @param handler the handler to be used if tasks cannot be executed.
     */
    public FastEventExecutorGroup(int nThreads, ThreadFactory factory, boolean addTaskWakeup, int maxPendingTasks, RejectedExecutionHandler handler) {
        super(nThreads, factory, addTaskWakeup, maxPendingTasks, handler);
    }

    /**
     * Creates a new FastEventExecutor using the provided parameters.
     *
     * @param executor the Executor used to execute tasks.
     * @param args additional arguments, including:
     *             1. Boolean indicating whether to add task wakeup.
     *             2. Integer specifying the maximum number of pending tasks.
     *             3. RejectedExecutionHandler to handle rejected tasks.
     * @return a new FastEventExecutor instance.
     * @throws Exception if an error occurs during the creation of the FastEventExecutor.
     */
    @Override
    protected EventExecutor newChild(Executor executor, Object... args) throws Exception {
        return new FastEventExecutor(this, executor,
                (Boolean) args[0], (Integer) args[1], (RejectedExecutionHandler) args[2]);
    }
}
