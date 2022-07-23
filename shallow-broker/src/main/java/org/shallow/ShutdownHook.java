package org.shallow;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import java.util.concurrent.Callable;

public class ShutdownHook<V> extends Thread{
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ShutdownHook.class);

    private volatile boolean stopped = false;
    private final Callable<V> callback;

    public ShutdownHook(Callable<V> callback) {
        super("Server shutdownHook");
        this.callback = callback;
    }

    @Override
    public void run() {
        synchronized (this) {
            if (logger.isInfoEnabled()) {
                logger.info("Server shutdownHooke was invoked");
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
