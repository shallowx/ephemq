package org.ostara.client.consumer;

import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.Semaphore;
import org.ostara.client.Message;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;

public class MessageProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageProcessor.class);

    private final String id;
    private final Semaphore semaphore;
    private final EventExecutor executor;

    public MessageProcessor(String id, Semaphore semaphore, EventExecutor executor) {
        this.id = id;
        this.semaphore = semaphore;
        this.executor = executor;
    }

    public void process(Message message, MessageListener listener, MessagePostInterceptor filter) {
        if (executor.isShutdown()) {
            logger.warn("Event executor is shutdown");
            return;
        }

        semaphore.acquireUninterruptibly();
        try {
            executor.execute(() -> doProcess(message, listener, filter));
        } catch (Throwable t) {
            logger.error("Handle message execute failed, message={}", message);
            semaphore.release();
        }
    }

    public void doProcess(Message message, MessageListener listener, MessagePostInterceptor mpInterceptor) {
        try {
            if (mpInterceptor != null) {
                message = mpInterceptor.interceptor(message);
            }

            listener.onMessage(message);
        } catch (Throwable t) {
            logger.error("Failed to handle message, message={}", message);
            semaphore.release();
        }
    }
}
