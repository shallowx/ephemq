package org.shallow.internal;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.network.MetadataSocketServer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public final class MetadataServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataServer.class);

    private final MetadataSocketServer socketServer;
    private final MetadataConfig config;
    private final CountDownLatch latch;
    private final MetadataManager manager;

    public MetadataServer(MetadataConfig config) {
        this.config = config;
        this.manager = new DefaultMetadataManager(config);
        this.socketServer = new MetadataSocketServer(config, manager);
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

        latch.await();
    }

    public void shutdownGracefully() throws Exception {
        manager.shutdownGracefully();
        socketServer.shutdownGracefully();
    }
}
