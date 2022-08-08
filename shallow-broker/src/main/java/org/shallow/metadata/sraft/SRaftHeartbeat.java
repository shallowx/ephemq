package org.shallow.metadata.sraft;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SRaftHeartbeat {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftHeartbeat.class);

    private final BrokerConfig config;
    private final EventExecutor scheduleHeartbeatTask;
    private long lastKeepHeartbeatTime;

    public SRaftHeartbeat(BrokerConfig config, EventExecutorGroup group) {
        this.config = config;
        this.scheduleHeartbeatTask = group.next();
    }

    public void registerHeartbeat() {
        scheduleHeartbeatTask.scheduleAtFixedRate(this::doRegisterHeartbeat,
                ThreadLocalRandom.current().nextInt(config.getHeartbeatRandomOriginTimeMs(),
                        config.getHeartbeatRandomBoundTimeMs()),
                config.getHeartIntervalTimeMs(), TimeUnit.MILLISECONDS
        );
    }

    private void doRegisterHeartbeat() {
        // TODO send heartbeat request to followers
    }
}
