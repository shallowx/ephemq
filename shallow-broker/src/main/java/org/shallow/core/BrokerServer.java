package org.shallow.core;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.remote.BrokerSocketServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public final class BrokerServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerServer.class);

    private final BrokerSocketServer socketServer;
    private final BrokerConfig config;
    private final CountDownLatch latch;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
        this.socketServer = new BrokerSocketServer(config);
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
        latch.await();
    }

    public void shutdownGracefully() throws Exception {
        socketServer.shutdownGracefully();
    }
}
