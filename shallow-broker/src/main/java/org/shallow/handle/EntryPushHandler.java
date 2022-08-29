package org.shallow.handle;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class EntryPushHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPushHandler.class);
    private final BrokerConfig config;

    public EntryPushHandler(BrokerConfig config) {
        this.config = config;
    }

    public void handle(){}

}
