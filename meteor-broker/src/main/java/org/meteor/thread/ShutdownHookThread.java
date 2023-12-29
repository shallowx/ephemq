package org.meteor.thread;

import org.meteor.common.logging.InternalLogger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class ShutdownHookThread {
    private final AtomicInteger shutdownTimes = new AtomicInteger(0);
    private final InternalLogger logger;
    private final Callable<?> callable;
    private volatile boolean hasShutdown = false;

    public ShutdownHookThread(InternalLogger logger, Callable<?> callable) {
        this.logger = logger;
        this.callable = callable;
    }

    public Thread newThread() {
        MeteorThreadFactory factory = new MeteorThreadFactory(getClass());
        return factory.newThread(this::run, "Shutdown-hook-thread");
    }

    public void run() {
        synchronized (this) {
            if (logger.isWarnEnabled()) {
                logger.warn("Shutdown hook was invoked, shutdown time(ms)[{}]", this.shutdownTimes.incrementAndGet());
            }

            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long begin = System.currentTimeMillis();
                try {
                    callable.call();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }

                long consumingTime = System.currentTimeMillis() - begin;
                if (logger.isWarnEnabled()) {
                    logger.warn("Shutdown hook over, consuming time(ms)[{}]", consumingTime);
                }
            }
        }
    }
}
