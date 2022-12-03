package org.ostara.common.thread;

import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;

import java.util.concurrent.Callable;

public class ShutdownHook<V> extends Thread {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ShutdownHook.class);

    private volatile boolean stopped = false;
    private final Callable<V> callback;
    private final String name;

    public ShutdownHook(String name, Callable<V> callback) {
        super(name);
        this.callback = callback;
        this.name = name;
    }

    @Override
    public void run() {
        synchronized (this) {
            if (logger.isInfoEnabled()) {
                logger.info("The {} shutdownHook was invoked", name);
            }

            if (!stopped) {
                this.stopped = true;
                try {
                    callback.call();
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            if (logger.isInfoEnabled()) {
                logger.info("The simple server was closed", name);
            }
        }
    }
}
