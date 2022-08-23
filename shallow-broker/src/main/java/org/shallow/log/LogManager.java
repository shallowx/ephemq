package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.Promise;
import org.shallow.RemoteException;
import org.shallow.consumer.Subscription;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.proto.server.SubscribeResponse;

import javax.annotation.concurrent.ThreadSafe;
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
            promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.subscribe(queue, ledgerId, epoch, index, promise);
    }

    public void append(int ledgerId, String queue, ByteBuf payload, Promise<Offset> promise) {
        Ledger ledger = logs.get(ledgerId);
        if (isNull(ledger)) {
            promise.tryFailure(RemoteException.of(RemoteException.Failure.MESSAGE_APPEND_EXCEPTION, String.format("Ledger %d not found", ledgerId)));
            return;
        }
        ledger.append(queue, payload, promise);
    }

    public Ledger getLog(int ledger) {
        return logs.get(ledger);
    }
}
