package org.meteor.thread;

import java.util.concurrent.Callable;
import org.meteor.common.logging.InternalLogger;

public class ShutdownHookThread {
    private final InternalLogger logger;
    private final Callable<?> callable;
    private volatile boolean hasShutdown = false;

    public ShutdownHookThread(InternalLogger logger, Callable<?> callable) {
        this.logger = logger;
        this.callable = callable;
    }

    public Thread newThread() {
        MeteorThreadFactory factory = new MeteorThreadFactory(getClass());
        return factory.newThread(this::run);
    }

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
                    logger.info("Shutdown hook over, consumed time(ms)[{}]", consumingTime);
                }
            }
        }
    }
}
