package org.ostara.client.consumer;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import static org.ostara.remote.util.NetworkUtils.switchSocketAddress;
import static org.ostara.remote.util.ProtoBufUtils.readProto;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutorGroup;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.ostara.client.Extras;
import org.ostara.client.Message;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientListener;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Subscription;
import org.ostara.remote.proto.notify.NodeOfflineSignal;
import org.ostara.remote.proto.notify.PartitionChangedSignal;
import org.ostara.remote.proto.server.SendMessageExtras;
import org.ostara.remote.util.ByteBufUtils;

final class MessageConsumerListener implements ClientListener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageConsumerListener.class);

    private MessageListener listener;
    private ConsumerInterceptor interceptor;
    private final MessageProcessor[] handlers;
    private final Map<Integer/*ledgerId*/, AtomicReference<Subscription>> subscriptionShips =
            new Int2ObjectOpenHashMap<>();
    private final MessageConsumer consumer;

    public MessageConsumerListener(ConsumerConfig consumerConfig, MessageConsumer consumer) {
        EventExecutorGroup group =
                newEventExecutorGroup(consumerConfig.getMessageHandleThreadLimit(), "client-message-handle");

        int n = tableSizeFor(consumerConfig.getMessageHandleThreadLimit());
        this.handlers = new MessageProcessor[n];
        for (int i = 0; i < n; i++) {
            Semaphore semaphore = new Semaphore(consumerConfig.getMessageHandleSemaphoreLimit());
            this.handlers[i] = new MessageProcessor(String.valueOf(i), semaphore, group.next());
        }
        this.consumer = consumer;
    }

    @Override
    public void registerListener(ConsumerListener listener) {
        this.listener = (MessageListener) listener;
    }

    public void registerInterceptor(ConsumerInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public void set(int epoch, long index, String queue, int ledger, short version) {
        try {
            synchronized (subscriptionShips) {
                Subscription subscription = Subscription
                        .newBuilder()
                        .epoch(epoch)
                        .index(index)
                        .queue(queue)
                        .ledger(ledger)
                        .version(version)
                        .build();
                AtomicReference<Subscription> reference = subscriptionShips.get(ledger);
                if (reference == null) {
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
        logger.debug("Receive partition changed signal, channel={} signal={}", channel.toString(),
                signal.toString());
        //do nothing
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        logger.debug("Receive node offline signal, channel={} signal={}", channel.toString(), signal.toString());

        String nodeId = signal.getNodeId();
        String host = signal.getHost();
        int port = signal.getPort();

        SocketAddress address = switchSocketAddress(host, port);
        consumer.resetSuscribe(address);
    }

    @Override
    public void onPushMessage(Channel channel, int ledgerId, short version, String topic, String queue, int epoch,
                              long index, ByteBuf data) {
        try {
            AtomicReference<Subscription> sequence = subscriptionShips.get(ledgerId);
            if (sequence == null) {
                logger.error(
                        "Channel consume sequence not initialize, channel={} ledgerId={} topic={} queue={} version={}"
                                + " epoch={} index={}",
                        channel.toString(), ledgerId, topic, queue, version, epoch, index);
                return;
            }

            Subscription preShip = sequence.get();
            if (preShip == null || ((epoch == preShip.getEpoch() && index > preShip.getIndex())
                    || epoch > preShip.getEpoch() || version > preShip.getVersion())) {

                Subscription theLastShip = Subscription
                        .newBuilder()
                        .epoch(epoch)
                        .index(index)
                        .queue(queue)
                        .ledger(ledgerId)
                        .version(version)
                        .build();

                if (!sequence.compareAndSet(preShip, theLastShip)) {
                    logger.debug("Chanel<{}> repeated message, last={} pre={}", channel.toString(), theLastShip,
                            preShip);
                }
            }

            MessageProcessor handler = handlers[hash(ledgerId, handlers.length)];

            SendMessageExtras extras = readProto(data, SendMessageExtras.parser());
            byte[] body = ByteBufUtils.buf2Bytes(data);
            Message message = new Message(topic, queue, version, body, epoch, index, new Extras(extras.getExtrasMap()));

            handler.process(message, listener, interceptor);
        } catch (Throwable t) {
            logger.error(
                    "Failed to handle subscribe message, channel={} ledgerId={} topic={} queue={} version={} "
                            + "epoch={} index={} , error={}",
                    channel.toString(), ledgerId, topic, queue, version, epoch, index, t);
        }
    }

    private int tableSizeFor(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        int maximum_capacity = 1 << 30;
        return (n < 0) ? 1 : (n >= maximum_capacity) ? maximum_capacity : n + 1;
    }

    private int hash(int input, int buckets) {
        long state = input & 0xffffffffL;

        int candidate = 0;
        int next;

        while (true) {
            next = (int) ((candidate + 1) / ((double) ((int) ((state = (2862933555777951757L * state)) >>> 33))
                    / 0x1.0p31));
            if (next >= 0 && next < buckets) {
                candidate = next;
            } else {
                return candidate;
            }
        }
    }
}
