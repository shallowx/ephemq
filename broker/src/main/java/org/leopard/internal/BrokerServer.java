package org.leopard.internal;

import io.netty.util.concurrent.ScheduledFuture;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.network.BrokerSocketServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;

public final class BrokerServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerServer.class);

    private final BrokerSocketServer socketServer;
    private final BrokerManager manager;

    public BrokerServer(BrokerConfig config) throws Exception {
        this.manager = new DefaultBrokerManager(config);
        this.socketServer = new BrokerSocketServer(config, manager);

    }

    public void start() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
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
