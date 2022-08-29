package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.shallow.RemoteException;
import org.shallow.consumer.pull.PullResult;
import org.shallow.consumer.push.Subscription;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.proto.server.PullMessageResponse;
import org.shallow.util.NetworkUtil;

import static org.shallow.util.ObjectUtil.isNull;

public class LedgerManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerManager.class);

    private final BrokerConfig config;
    private final DistributedAtomicInteger atomicLedgerValue;
    private final DistributedAtomicInteger atomicRequestValue;
    private final Int2ObjectMap<Ledger> ledgers = new Int2ObjectOpenHashMap<>();

    public LedgerManager(BrokerConfig config) {
        this.config = config;
        this.atomicLedgerValue = new DistributedAtomicInteger();
        this.atomicRequestValue = new DistributedAtomicInteger();
    }

    public void initLog(String topic, int partition, int epoch) {
        Integer ledgerId = atomicLedgerValue.increment().preValue();
        Ledger ledger = ledgers.computeIfAbsent(ledgerId, k -> new Ledger(config, topic, partition, ledgerId, epoch));

        if (logger.isInfoEnabled()) {
            logger.info("Initialize log successfully, topic={} partition={} ledger={}", topic, partition, ledgerId);
        }
        ledgers.putIfAbsent(ledgerId, ledger);
    }

    public void subscribe(Channel channel, String queue, int ledgerId, int epoch, long index, Promise<Subscription> promise) {
        Ledger ledger = getLedger(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.subscribe(channel, queue, epoch, index, promise);
    }

    public void append(int ledgerId, String queue, ByteBuf payload, short version, Promise<Offset> promise) {
        Ledger ledger = getLedger(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.append(queue, version, payload, promise);
    }

    @SuppressWarnings("all")
    public void pull(Channel channel, int ledgerId, String queue, short version, int epoch, long index, int limit, Promise<PullMessageResponse> promise) {
        Ledger ledger = getLedger(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_PULL_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        Promise<PullResult> pullResultPromise = NetworkUtil.newImmediatePromise();
        pullResultPromise.addListeners((GenericFutureListener<Future<PullResult>>) future -> {
            if (future.isSuccess()) {
                PullResult result = future.get();
                String topic = this.getLedger(ledgerId).getTopic();
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
        ledger.pull(requestId, channel, queue, version, epoch, index, limit, pullResultPromise);
    }

    public Ledger getLedger(int ledgerId) {
        return ledgers.get(ledgerId);
    }

    public void close() {
        if (!ledgers.isEmpty()) {
            ledgers.values().forEach(Ledger::close);
        }
    }
}
