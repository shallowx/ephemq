package org.shallow.consumer.push;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.Message;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.concurrent.Semaphore;

public class MessageHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageHandler.class);

    private final String id;
    private final Semaphore semaphore;
    private final EventExecutor executor;

    public MessageHandler(String id, Semaphore semaphore, EventExecutor executor) {
        this.id = id;
        this.semaphore = semaphore;
        this.executor = executor;
    }

    public void handle(Message message, MessagePushListener listener, MessagePostFilter filter) {
        if (executor.isShutdown()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Event executor is shutdown");
            }
            return;
        }

        semaphore.acquireUninterruptibly();
        try {
            executor.execute(() -> doHandle(message, listener, filter));
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Handle message execute failed, message={}", message);
            }
            semaphore.release();
        }
    }

    public void doHandle(Message message, MessagePushListener listener, MessagePostFilter filter) {
        try {
            if (filter != null) {
                message = filter.filter(message);
            }

            listener.onMessage(message);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to handle message, message={}", message);
            }
            semaphore.release();
        }
    }
}
