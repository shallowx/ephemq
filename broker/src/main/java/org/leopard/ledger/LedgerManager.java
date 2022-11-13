package org.leopard.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import org.leopard.internal.metrics.LedgerMetricsListener;
import org.leopard.remote.proto.server.PullMessageResponse;
import org.leopard.remote.RemoteException;
import org.leopard.client.consumer.pull.PullResult;
import org.leopard.client.consumer.push.Subscription;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.atomic.DistributedAtomicInteger;
import org.leopard.remote.util.NetworkUtil;

import java.util.LinkedList;
import java.util.List;

public class LedgerManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerManager.class);

    private final BrokerConfig config;
    private final DistributedAtomicInteger atomicRequestValue;
    private final Int2ObjectMap<Ledger> ledgers = new Int2ObjectOpenHashMap<>();
    private final List<LedgerMetricsListener> listeners = new LinkedList<>();

    public LedgerManager(BrokerConfig config) {
        this.config = config;
        this.atomicRequestValue = new DistributedAtomicInteger();
    }

    public void start() throws Exception {
    }

    public void initLog(String topic, int partition, int epoch, int ledgerId, Promise<Void> promise) {
        try {
            Ledger ledger = ledgers.computeIfAbsent(ledgerId, k -> new Ledger(config, topic, partition, ledgerId, epoch));
            ledger.start();

            ledgers.putIfAbsent(ledgerId, ledger);

            if (logger.isInfoEnabled()) {
                logger.info("Initialize log successfully, topic={} partition={} ledger={}", topic, partition, ledgerId);
            }

            for (LedgerMetricsListener listener : listeners) {
                listener.onInitLedger(ledger);
            }

            promise.trySuccess(null);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Initialize log failed, topic={} partition={} ledger={}. error:{}", topic, partition, ledgerId, t);
            }
            promise.tryFailure(t);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void subscribe(Channel channel, String topic, String queue, short version, int ledgerId, int epoch, long index, Promise<Subscription> promise) {
        Ledger ledger = getLedger(ledgerId);
       try {
           if (ledger == null) {
               promise.tryFailure(RemoteException.of(RemoteException.Failure.SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
               return;
           }

           checkLedgerState(ledger);

           ledger.subscribe(channel, topic, queue, version, epoch, index, promise);
       } catch (Throwable t) {
           if (logger.isErrorEnabled()) {
               logger.error("Failed to subscribe, channel={} topic={} queue={} version={} epoch={} index={}", channel.toString(), ledger.getTopic(), queue, version, epoch, index);
           }
           promise.tryFailure(t);
       }
    }

    @SuppressWarnings("ConstantConditions")
    public void clean(Channel channel, String topic, String queue, int ledgerId, Promise<Void> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }
            checkLedgerState(ledger);

            ledger.clean(channel, topic, queue, promise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to clean subscribe, channel={} topic={} queue={} version={} epoch={} index={}", channel.toString(), ledger.getTopic(), queue);
            }
            promise.tryFailure(t);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public void append(int ledgerId, String queue, ByteBuf payload, short version, Promise<Offset> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }

            checkLedgerState(ledger);

            for (LedgerMetricsListener listener : listeners) {
                listener.onReceiveMessage(ledger.getTopic(), queue, ledger.getLedgerId(), 1);
            }

            ledger.append(queue, version, payload, promise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to append message, topic={} queue={} version={}, error:{}", ledger.getTopic(), queue, version, t);
            }
            promise.tryFailure(t);
        }
    }

    @SuppressWarnings({"ConstantConditions", "unchecked"})
    public void pull(Channel channel, int ledgerId, String topic, String queue, short version, int epoch, long index, int limit, Promise<PullMessageResponse> promise) {
        Ledger ledger = getLedger(ledgerId);
        try {
            if (ledger == null) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_PULL_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
                return;
            }

            Promise<PullResult> pullResultPromise = NetworkUtil.newImmediatePromise();
            pullResultPromise.addListeners((GenericFutureListener<Future<PullResult>>) future -> {
                if (future.isSuccess()) {
                    PullResult result = future.get();
                    result.setTopic(topic);

                    PullMessageResponse response = PullMessageResponse
                            .newBuilder()
                            .setTopic(topic)
                            .setEpoch(result.getStartEpoch())
                            .setIndex(result.getStartIndex())
                            .setLedger(ledgerId)
                            .setLimit(limit)
                            .setQueue(queue)
                            .build();
                    promise.trySuccess(response);
                } else {
                    promise.tryFailure(future.cause());
                }
            });
            int requestId = atomicRequestValue.increment().preValue();
            ledger.pull(requestId, channel, topic, queue, version, epoch, index, limit, pullResultPromise);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to pull message, ledger={} topic={} queue={} version={} epoch={} index={}", ledgerId, ledger.getTopic(), queue, version, epoch, index);
            }
            promise.tryFailure(t);
        }
    }

    public void clearChannel(Channel channel) {
        if (ledgers.isEmpty()) {
            return;
        }

        ObjectCollection<Ledger> activeLedgers = ledgers.values();
        for (Ledger ledger : activeLedgers) {
            ledger.clearChannel(channel);
        }
    }

    public Ledger getLedger(int ledgerId) {
        return ledgers.get(ledgerId);
    }

    private void checkLedgerState(Ledger ledger) {
        Ledger.State state = ledger.getState();
        if (state != Ledger.State.STARTED) {
           throw new RuntimeException(String.format("Ledger %d not active", ledger.getLedgerId()));
        }
    }

    public void close() throws Exception {
        if (!ledgers.isEmpty()) {
            ledgers.values().forEach(Ledger::close);
        }

        if (logger.isWarnEnabled()) {
            logger.warn("Ledger manager close is starting...");
        }
    }
}
