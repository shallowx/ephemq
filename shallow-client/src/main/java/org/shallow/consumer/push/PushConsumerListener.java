package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutorGroup;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.shallow.Message;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.internal.Listener;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.proto.server.SendMessageExtras;
import org.shallow.util.ByteBufUtil;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.ProtoBufUtil.readProto;

final class PushConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerListener.class);

    private MessagePushListener listener;
    private MessagePostFilter   filter;
    private final MessageHandler[] handlers;
    private final Map<Integer, AtomicReference<Subscription>> subscriptionShips = new Int2ObjectOpenHashMap<>();

    public PushConsumerListener(ConsumerConfig consumerConfig) {
        EventExecutorGroup group = newEventExecutorGroup(1, "client-message-handle");

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

    public void set(int epoch, long index, String queue, int ledger) {
        Subscription subscription = new Subscription(epoch, index, queue, ledger);
        AtomicReference<Subscription> reference = subscriptionShips.get(ledger);
        if (reference == null){
            reference = new AtomicReference<>();
        }
        reference.set(subscription);
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive partition changed signal, channel={} signal={}", channel.toString(), signal.toString());
        }
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive node offline signal, channel={} signal={}", channel.toString(), signal.toString());
        }
    }

    @Override
    public void onPushMessage(Channel channel, int ledgerId, short version, String topic, String queue, int epoch, long index, ByteBuf data) {
        try {
            SendMessageExtras extras = readProto(data, SendMessageExtras.parser());

            byte[] body = ByteBufUtil.buf2Bytes(data);
            Message message = new Message(topic, queue, version, body, epoch, index, new Message.Extras(extras.getExtrasMap()));

            Subscription theLastShip = new Subscription(epoch, index, queue, ledgerId);

            AtomicReference<Subscription> sequence = subscriptionShips.get(ledgerId);
            if (sequence == null) {
                logger.error("Channel consume sequence not initialize, channel={} ledgerId={} topic={} queue={} version={} epoch={} index={}",
                        channel.toString(), ledgerId, topic, queue, version, epoch, index);
                return;
            }

            Subscription preShip = sequence.get();
            if (preShip == null || ((epoch == preShip.epoch() && index > theLastShip.index()) || epoch > theLastShip.epoch())) {
                if (!sequence.compareAndSet(preShip, theLastShip)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Chanel<{}> repeated message, last={} pre={}", channel.toString(), theLastShip, preShip);
                    }
                }
            }

            MessageHandler handler = handlers[(Objects.hash(topic, queue) & 0x7fffffff) % handlers.length];
            handler.handle(message, listener, filter);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to handle subscribe message, channel={} ledgerId={} topic={} queue={} version={} epoch={} index={} , error={}",
                        channel.toString(), ledgerId, topic, queue, version, epoch, index, t);
            }
        }
    }
}
