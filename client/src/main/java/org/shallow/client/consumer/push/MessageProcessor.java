package org.shallow.client.consumer.push;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.client.Message;
import org.shallow.client.consumer.MessagePostInterceptor;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

import java.util.concurrent.Semaphore;

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

    public void process(Message message, MessagePushListener listener, MessagePostInterceptor filter) {
        if (executor.isShutdown()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Event executor is shutdown");
            }
            return;
        }

        semaphore.acquireUninterruptibly();
        try {
            executor.execute(() -> doProcess(message, listener, filter));
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Handle message execute failed, message={}", message);
            }
            semaphore.release();
        }
    }

    public void doProcess(Message message, MessagePushListener listener, MessagePostInterceptor mpInterceptor) {
        try {
            if (mpInterceptor != null) {
                message = mpInterceptor.interceptor(message);
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
