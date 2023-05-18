package org.ostara.context;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.config.ServerConfig;
import org.ostara.network.SimpleSocketServer;

public final class SimpleServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SimpleServer.class);

    private final SimpleSocketServer socketServer;
    private final ResourceContext context;

    public SimpleServer(ServerConfig config) throws Exception {
        this.context = new DefaultResourceContext(config);
        this.socketServer = new SimpleSocketServer(config, context);

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

        context.start();

        latch.await();
        if (logger.isInfoEnabled()) {
            logger.info("The broker server started successfully");
        }
    }

    public void shutdownGracefully() throws Exception {
        context.shutdownGracefully();
        socketServer.shutdownGracefully();
    }
}
