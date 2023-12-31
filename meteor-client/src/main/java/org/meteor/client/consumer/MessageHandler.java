package org.meteor.client.consumer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Extras;
import org.meteor.common.message.MessageId;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.util.ProtoBufUtil;

import java.util.Map;
import java.util.concurrent.Semaphore;

public class MessageHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageHandler.class);
    private final String id;
    private final Semaphore semaphore;
    private final EventExecutor handleExecutor;
    private final Map<String, Map<String, Mode>> wholeQueueTopics;
    private final MessageListener listener;

    public MessageHandler(String id, Semaphore semaphore, EventExecutor executor, Map<String,
            Map<String, Mode>> wholeQueueTopics, MessageListener listener) {
        this.id = id;
        this.semaphore = semaphore;
        this.handleExecutor = executor;
        this.wholeQueueTopics = wholeQueueTopics;
        this.listener = listener;
    }

    void handle(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
        if (handleExecutor.isShuttingDown()) {
            return;
        }

        semaphore.acquireUninterruptibly();
        data.retain();

        try {
            handleExecutor.execute((AbstractEventExecutor.LazyRunnable) () -> doHandle(channel, marker, messageId, data));
        } catch (Throwable t) {
            data.release();
            semaphore.release();
            if (logger.isErrorEnabled()) {
                logger.error("Consumer[{}] handle message failed", id, t);
            }
        }
    }

    private void doHandle(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
        int length = data.readableBytes();
        try {
            MessageMetadata metadata = ProtoBufUtil.readProto(data, MessageMetadata.parser());
            String topic = metadata.getTopic();
            String queue = metadata.getQueue();
            Map<String, Mode> topicModes = wholeQueueTopics.get(queue);
            Mode mode = topicModes == null ? null : topicModes.get(topic);
            if (mode == null || mode == Mode.DELETE) {
                return;
            }

            Extras extras = new Extras(metadata.getExtrasMap());
            listener.onMessage(topic, queue, messageId, data, extras);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(" Consumer handle message failed, address[{}] marker[{}] messageId[{}] length[{}]", channel.address(), marker, messageId, length, t);
            }
        } finally {
            data.release();
            semaphore.release();
        }
    }
}
