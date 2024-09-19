package org.meteor.thread;

import java.util.concurrent.Callable;
import org.meteor.common.logging.InternalLogger;

/**
 * A thread that acts as a shutdown hook and ensures that a specified
 * task is executed exactly once when the application is shutting down.
 */
public class ShutdownHookThread {
    /**
     * A logger used to log messages related to the execution and status of the shutdown hook.
     * This logger provides different levels of logging such as trace, debug, info, warn, and error.
     * It is used to report the success or failure of the shutdown task, as well as any exceptions thrown.
     */
    private final InternalLogger logger;
    /**
     * The callable task that is invoked when the shutdown hook thread runs.
     * This task is executed exactly once during the shutdown process.
     */
    private final Callable<?> callable;
    /**
     * A flag to indicate whether the shutdown process has been executed.
     * Once set to true, it prevents the shutdown task from being invoked more than once.
     */
    private volatile boolean hasShutdown = false;

    /**
     * Creates a ShutdownHookThread instance.
     *
     * @param logger   The logger to use for logging messages during shutdown.
     * @param callable The callable task to execute exactly once when the application is shutting down.
     */
    public ShutdownHookThread(InternalLogger logger, Callable<?> callable) {
        this.logger = logger;
        this.callable = callable;
    }

    /**
     * Creates a new thread using the MeteorThreadFactory.
     *
     * @return a new Thread configured to execute the run method of this instance
     */
    public Thread newThread() {
        MeteorThreadFactory factory = new MeteorThreadFactory(getClass());
        return factory.newThread(this::run);
    }

    /**
     * Executes the shutdown hook. Ensures that the specified callable task is executed exactly once,
     * even if called multiple times. The task execution is synchronized and protected against
     * concurrent invocations. Logs relevant information and errors during the process.
     * <p>
     * The method performs the following steps:
     * 1. Checks if the shutdown task has already been executed. If it has, it returns immediately.
     * 2. Marks the task as executed to prevent future invocations.
     * 3. Records the start time of the task execution.
     * 4. Attempts to execute the provided callable task.
     * 5. Logs any exceptions encountered during the task execution if error logging is enabled.
     * 6. Calculates and logs the time consumed by the task execution if info logging is enabled.
     */
    public void run() {
        synchronized (this) {
            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long begin = System.currentTimeMillis();
                try {
                    callable.call();
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error(e.getMessage(), e);
                    }
                }

                long consumingTime = System.currentTimeMillis() - begin;
                if (logger.isInfoEnabled()) {
                    logger.info(STR."Shutdown hook over, consumed time(ms)[\{consumingTime}]");
                }
            }
        }
    }
}
