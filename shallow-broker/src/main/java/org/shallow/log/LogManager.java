package org.shallow.log;

import io.netty.util.collection.IntObjectHashMap;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.Map;

public class LogManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LogManager.class);

    private final Map<Integer, Log> logs = new IntObjectHashMap<>();

    public void initLog() {
        int ledger = 0;

        logs.put(ledger, null);
        if (logger.isInfoEnabled()) {
            logger.info("Log<ledger={}> initialize successfully", ledger);
        }
    }

    public Log getLog(int ledger) {
        return logs.get(ledger);
    }
}
