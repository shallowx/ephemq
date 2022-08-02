package org.shallow.hook;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.concurrent.Callable;

public class ShutdownHook<V> extends Thread{
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ShutdownHook.class);

    private volatile boolean stopped = false;
    private final Callable<V> callback;
    private String name;

    public ShutdownHook(String name ,Callable<V> callback) {
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
        }
    }
}
