package org.shallow.nameserver;

import io.netty.util.concurrent.ScheduledFuture;
import org.shallow.DefaultManager;
import org.shallow.Manager;
import org.shallow.NameserverConfig;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.shallow.remote.util.NetworkUtil.newEventExecutorGroup;

public final class NameCoreServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(NameCoreServer.class);

    private final NameserverSocketServer socketServer;

    public NameCoreServer(NameserverConfig config) throws Exception {
        Manager manager = new DefaultManager();
        this.socketServer = new NameserverSocketServer(config, manager);
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

        latch.await();
        if (logger.isInfoEnabled()){
            logger.info("The name server started successfully");
        }
    }

    public void shutdownGracefully() throws Exception {
        socketServer.shutdownGracefully();
    }
}
