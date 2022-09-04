package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.Message;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.internal.Listener;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.proto.server.SendMessageExtras;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.NetworkUtil;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;

import static org.shallow.util.ProtoBufUtil.readProto;

final class PushConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerListener.class);

    private MessagePushListener listener;
    private MessagePostFilter   filter;
    private MessageHandler[] handlers;

    public PushConsumerListener(ConsumerConfig consumerConfig) {
        EventExecutorGroup group = NetworkUtil.newEventExecutorGroup(1, "client-message-handle");

        handlers = new MessageHandler[consumerConfig.getMessageHandleThreadLimit()];
        for (int i = 0; i < consumerConfig.getMessageHandleThreadLimit(); i++) {
            Semaphore semaphore = new Semaphore(consumerConfig.messageHandleSemaphoreLimit);
            handlers[i] = new MessageHandler(String.valueOf(i), semaphore, group.next());
        }
    }

    public void registerListener(MessagePushListener listener) {
        this.listener = listener;
    }

    public void registerFilter(MessagePostFilter filter) {
        this.filter = filter;
    }


    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {

    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {

    }

    @Override
    public void onPushMessage(short version, String topic, String queue, int epoch, long index, ByteBuf data) {
        try {
            SendMessageExtras extras = readProto(data, SendMessageExtras.parser());

            byte[] body = ByteBufUtil.buf2Bytes(data);
            Message message = new Message(topic, queue, version, body, epoch, index, new Message.Extras(extras.getExtrasMap()));

            MessageHandler handler = handlers[(Objects.hash(topic, queue) & 0x7fffffff) % handlers.length];
            handler.handle(message, listener, filter);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }
}
