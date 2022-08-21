package org.shallow.log;

import io.netty.util.collection.IntObjectHashMap;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.internal.atomic.DistributedAtomicInteger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;

@ThreadSafe
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
        Integer ledger = atomicValue.increment().postValue();
        Ledger log = new Ledger(config, topic, partition, ledger, epoch);

        if (logger.isInfoEnabled()) {
            logger.info("Initialize log successfully, topic={} partition={} ledger={}", topic, partition, ledger);
        }
        logs.putIfAbsent(ledger, log);
    }

    public void append() {

    }

    public Ledger getLog(int ledger) {
        return logs.get(ledger);
    }
}
