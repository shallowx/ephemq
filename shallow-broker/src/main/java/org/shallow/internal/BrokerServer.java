package org.shallow.internal;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.network.BrokerSocketServer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

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
        CompletableFuture<Void> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                socketServer.start();
                future.complete(null);
                socketServer.awaitShutdownGracefully();
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to start socket server", e);
                }
                future.completeExceptionally(e);
            }
            latch.countDown();
        }, "socket-server-start").start();
        future.get();

        manager.start();

        if (logger.isInfoEnabled()){
            logger.info("The broker server started successfully");
        }
        latch.await();
    }

    public void shutdownGracefully() throws Exception {
        manager.shutdownGracefully();
        socketServer.shutdownGracefully();
    }
}
