package org.shallow.log;

import io.netty.util.collection.IntObjectHashMap;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.Map;

public class LogManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LogManager.class);

    private static final Map<Integer, Log> logs = new IntObjectHashMap<>();
    private final BrokerConfig config;

    public LogManager(BrokerConfig config) {
        this.config = config;
    }

    public void initLog(int partitions) {
        Log log = new Log();
        int ledger = log.getLedgerId();

        logs.put(ledger, log);
        if (logger.isInfoEnabled()) {
            logger.info("Log<ledger={}> initialize successfully", ledger);
        }
    }

    public Log getLog(int ledger) {
        return logs.get(ledger);
    }
}
