package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.RemoteException;
import org.shallow.consumer.pull.PullResult;
import org.shallow.consumer.push.Subscription;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.proto.server.PullMessageResponse;
import org.shallow.util.NetworkUtil;

import java.util.Map;

import static org.shallow.util.ObjectUtil.isNull;

public class LogManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LogManager.class);

    private static final Map<Integer, Ledger> logs = new IntObjectHashMap<>();
    private final BrokerConfig config;
    private final DistributedAtomicInteger atomicValue;

    public LogManager(BrokerConfig config) {
        this.config = config;
        this.atomicValue = new DistributedAtomicInteger();
    }

    public void initLog(String topic, int partition, int epoch) {
        Integer ledger = atomicValue.increment().preValue();
        Ledger log = new Ledger(config, topic, partition, ledger, epoch);

        if (logger.isInfoEnabled()) {
            logger.info("Initialize log successfully, topic={} partition={} ledger={}", topic, partition, ledger);
        }
        logs.putIfAbsent(ledger, log);
    }

    public void subscribe(String queue, int ledgerId, int epoch, long index, Promise<Subscription> promise) {
        Ledger ledger = logs.get(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.SUBSCRIBE_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.subscribe(queue, epoch, index, promise);
    }

    public void append(int ledgerId, String queue, ByteBuf payload, Promise<Offset> promise) {
        Ledger ledger = logs.get(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.append(queue, payload, promise);
    }

    @SuppressWarnings("all")
    public void pull(Channel channel, int ledgerId, String queue, int epoch, long index, int limit, Promise<PullMessageResponse> promise) {
        Ledger ledger = logs.get(ledgerId);
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
        ledger.pull(channel, queue, epoch, index, limit, pullResultPromise);
    }

    public Ledger getLedger(int ledger) {
        return logs.get(ledger);
    }
}
