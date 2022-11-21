package org.leopard.internal;

import io.netty.util.concurrent.ScheduledFuture;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.config.ServerConfig;
import org.leopard.network.SimpleSocketServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;

public final class SimpleServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SimpleServer.class);

    private final SimpleSocketServer socketServer;
    private final ResourceContext manager;

    public SimpleServer(ServerConfig config) throws Exception {
        this.manager = new DefaultResourceContext(config);
        this.socketServer = new SimpleSocketServer(config, manager);

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
        if (logger.isInfoEnabled()) {
            logger.info("The broker server started successfully");
        }
    }

    public void shutdownGracefully() throws Exception {
        manager.shutdownGracefully();
        socketServer.shutdownGracefully();
    }
}
