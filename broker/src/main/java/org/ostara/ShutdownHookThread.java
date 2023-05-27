package org.ostara;

import org.ostara.common.logging.InternalLogger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class ShutdownHookThread extends Thread {
    private volatile boolean hasShutdown = false;
    private final AtomicInteger shutdownTimes = new AtomicInteger(0);
    private final InternalLogger logger;
    private final Callable<?> callable;
    public ShutdownHookThread(InternalLogger logger, Callable<?> callable) {
        super("Shutdown-hook-thread");
        this.logger = logger;
        this.callable = callable;
    }
    @Override
    public void run() {
        synchronized (this) {
            logger.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());

            if (!this.hasShutdown) {
                this.hasShutdown = true;
                long begin = System.currentTimeMillis();
                try {
                    callable.call();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }

                long consumingTime = System.currentTimeMillis() - begin;
                logger.info("Shutdown hook over, consuming time(ms):{}", consumingTime);
            }
        }
    }
}
