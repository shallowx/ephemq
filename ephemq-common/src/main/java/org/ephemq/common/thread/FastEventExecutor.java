package org.ephemq.common.thread;

import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * FastEventExecutor is a specialized implementation of SingleThreadEventExecutor designed
 * to handle tasks quickly and efficiently.
 * <p>
 * The constructor provides multiple ways to initialize the executor with various parameters
 * including thread factory and rejected execution handler.
 * <p>
 * The newTaskQueue method creates a new task queue based on the maximum number of pending tasks,
 * choosing between a LinkedBlockingQueue or MpscBlockingConsumerArrayQueue.
 * <p>
 * The run method processes tasks sequentially, ensuring each task is run safely and updates
 * the last execution time before checking for shutdown confirmation.
 */
public class FastEventExecutor extends SingleThreadEventExecutor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(FastEventExecutor.class);

    /**
     * Creates a new FastEventExecutor with the provided ThreadFactory.
     *
     * @param factory the ThreadFactory to be used for creating new threads.
     */
    public FastEventExecutor(ThreadFactory factory) {
        this(null, factory, true, Integer.MAX_VALUE, RejectedExecutionHandlers.reject());
    }

    /**
     * Constructs a new FastEventExecutor instance.
     *
     * @param parent The parent EventExecutorGroup to which this executor belongs. It can be null.
     * @param factory The factory to be used for creating new threads.
     * @param addTaskWakeup True if a task wakeup should be added, false otherwise.
     * @param maxPendingTasks The maximum number of tasks to be held in the pending queue.
     * @param rejectedExecutionHandler The handler to be used if tasks cannot be executed.
     */
    public FastEventExecutor(EventExecutorGroup parent, ThreadFactory factory, boolean addTaskWakeup,
                             int maxPendingTasks, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, factory, addTaskWakeup, maxPendingTasks, rejectedExecutionHandler);
    }

    /**
     * Constructs a new FastEventExecutor.
     *
     * @param parent                   the parent EventExecutorGroup, may be null
     * @param executor                 the Executor used to execute tasks
     * @param addTaskWakeup            whether or not the executor should wake up on task addition
     * @param maxPendingTasks          the maximum number of pending tasks allowed in the queue
     * @param rejectedExecutionHandler the handler to use when tasks are rejected
     */
    public FastEventExecutor(EventExecutorGroup parent, Executor executor, boolean addTaskWakeup, int maxPendingTasks, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, addTaskWakeup, maxPendingTasks, rejectedExecutionHandler);
    }

    /**
     * Creates a new task queue with a given maximum number of pending tasks.
     * If the number of pending tasks is less than or equal to zero or greater than
     * 1 << 30, it returns a LinkedBlockingQueue. Otherwise, it returns a
     * MpscBlockingConsumerArrayQueue with the specified maximum size.
     *
     * @param maxPendingTasks the maximum number of pending tasks allowed in the queue.
     *                        If the value is less than or equal to zero or exceeds 1 << 30,
     *                        a LinkedBlockingQueue will be used.
     * @return a new Queue instance for managing Runnable tasks.
     */
    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        if (maxPendingTasks <= 0 || maxPendingTasks > (1 << 30)) {
            return new LinkedBlockingQueue<>();
        }

        return new MpscBlockingConsumerArrayQueue<>(maxPendingTasks);
    }

    /**
     * This method constantly takes tasks from a task queue and executes them in a loop until
     * it confirms that the executor should shutdown. It handles any exceptions thrown by the
     * tasks by logging the error messages and the associated throwables. After executing each
     * task, it updates the last execution time to keep track of the last processed task.
     */
    @Override
    protected void run() {
        do {
            Runnable task = takeTask();
            if (task != null) {
                try {
                    task.run();
                } catch (Throwable t) {
                    logger.error(t.getMessage(), t);
                }
                updateLastExecutionTime();
            }
        } while (!confirmShutdown());
    }
}
