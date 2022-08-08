package org.shallow.internal;

import io.netty.util.concurrent.ScheduledFuture;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.network.BrokerSocketServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

public final class BrokerServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerServer.class);

    private final BrokerSocketServer socketServer;
    private final BrokerConfig config;
    private final CountDownLatch latch;
    private final BrokerManager manager;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
        this.manager = new DefaultBrokerManager(config);
        this.socketServer = new BrokerSocketServer(config, manager);
        this.latch = new CountDownLatch(1);
    }

    public void start() throws Exception {
        ScheduledFuture<?> socketStartFuture = newEventExecutorGroup(1, "socket-start")
                .next()
                .schedule(() -> {
                    try {
                        socketServer.start();
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to start socket server", e);
                        }
                        socketServer.awaitShutdownGracefully();
                    }
                    latch.countDown();
                }, 0, TimeUnit.MILLISECONDS);
        socketStartFuture.get();

        manager.start();

        latch.await();
        if (logger.isInfoEnabled()){
            logger.info("The broker server started successfully");
        }
    }

    public void shutdownGracefully() throws Exception {
        manager.shutdownGracefully();
        socketServer.shutdownGracefully();
    }
}
