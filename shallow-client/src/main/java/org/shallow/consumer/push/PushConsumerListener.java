package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutorGroup;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.shallow.Extras;
import org.shallow.Message;
import org.shallow.consumer.ConsumeListener;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.consumer.MessagePostInterceptor;
import org.shallow.internal.Listener;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.proto.server.SendMessageExtras;
import org.shallow.util.ByteBufUtil;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.switchSocketAddress;
import static org.shallow.util.ProtoBufUtil.readProto;

final class PushConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerListener.class);

    private MessagePushListener listener;
    private MessagePostInterceptor interceptor;
    private final MessageProcessor[] handlers;
    private final Map<Integer/*ledgerId*/, AtomicReference<Subscription>> subscriptionShips = new Int2ObjectOpenHashMap<>();
    private final MessagePushConsumer pushConsumer;

    public PushConsumerListener(ConsumerConfig consumerConfig, MessagePushConsumer consumer) {
        EventExecutorGroup group = newEventExecutorGroup(consumerConfig.getMessageHandleThreadLimit(), "client-message-handle");

        handlers = new MessageProcessor[consumerConfig.getMessageHandleThreadLimit()];
        for (int i = 0; i < consumerConfig.getMessageHandleThreadLimit(); i++) {
            Semaphore semaphore = new Semaphore(consumerConfig.messageHandleSemaphoreLimit);
            handlers[i] = new MessageProcessor(String.valueOf(i), semaphore, group.next());
        }
        this.pushConsumer = consumer;
    }

    @Override
    public void registerListener(ConsumeListener listener) {
        this.listener = (MessagePushListener) listener;
    }

    public void registerInterceptor(MessagePostInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void set(int epoch, long index, String queue, int ledger, short version) {
        try {
            synchronized (subscriptionShips) {
                Subscription subscription = new Subscription(epoch, index, queue, ledger, version);
                AtomicReference<Subscription> reference = subscriptionShips.get(ledger);
                if (reference == null){
                    reference = new AtomicReference<>();
                }
                reference.set(subscription);
                subscriptionShips.put(ledger, reference);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Failed to handle message subscribe sequence");
        }
    }

    public AtomicReference<Subscription> getSubscriptionShip(int ledger) {
        return subscriptionShips.get(ledger);
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive partition changed signal, channel={} signal={}", channel.toString(), signal.toString());
        }
        //do nothing
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive node offline signal, channel={} signal={}", channel.toString(), signal.toString());
        }

        String nodeId = signal.getNodeId();
        String host = signal.getHost();
        int port = signal.getPort();

        SocketAddress address = switchSocketAddress(host, port);
        pushConsumer.resetSuscribe(address);
    }

    @Override
    public void onPushMessage(Channel channel, int ledgerId, short version, String topic, String queue, int epoch, long index, ByteBuf data) {
        try {
            SendMessageExtras extras = readProto(data, SendMessageExtras.parser());

            byte[] body = ByteBufUtil.buf2Bytes(data);
            Message message = new Message(topic, queue, version, body, epoch, index, new Extras(extras.getExtrasMap()));

            Subscription theLastShip = new Subscription(epoch, index, queue, ledgerId, version);

            AtomicReference<Subscription> sequence = subscriptionShips.get(ledgerId);
            if (sequence == null) {
                logger.error("Channel consume sequence not initialize, channel={} ledgerId={} topic={} queue={} version={} epoch={} index={}",
                        channel.toString(), ledgerId, topic, queue, version, epoch, index);
                return;
            }

            Subscription preShip = sequence.get();
            if (preShip == null || ((epoch == preShip.epoch() && index > theLastShip.index()) ||
                            epoch > theLastShip.epoch() ||
                            version > theLastShip.version())) {
                if (!sequence.compareAndSet(preShip, theLastShip)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Chanel<{}> repeated message, last={} pre={}", channel.toString(), theLastShip, preShip);
                    }
                }
            }

            MessageProcessor handler = handlers[((Objects.hash(topic, queue) + ledgerId) & 0x7fffffff) % handlers.length];
            handler.process(message, listener, interceptor);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to handle subscribe message, channel={} ledgerId={} topic={} queue={} version={} epoch={} index={} , error={}",
                        channel.toString(), ledgerId, topic, queue, version, epoch, index, t);
            }
        }
    }
}
