package org.meteor.internal;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.listener.ServerListener;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.ClusterManager;
import org.meteor.support.Manager;

public class MeteorServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorServer.class);
    private static final ExecutorService socketServerExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("socket-server").factory()
    );
    private final List<ServerListener> serverListeners = new ObjectArrayList<>();
    private final CountDownLatch countDownLatch;
    private final DefaultSocketServer defaultSocketServer;
    private final Manager manager;

    public MeteorServer(DefaultSocketServer defaultSocketServer, Manager manager) {
        this.defaultSocketServer = defaultSocketServer;
        this.manager = manager;
        this.countDownLatch = new CountDownLatch(1);
    }

    public void addListener(@Nonnull ServerListener listener) {
        serverListeners.add(listener);
    }

    public void start() throws Exception {
        CompletableFuture<Void> startFuture = new CompletableFuture<>();
        socketServerExecutor.execute(() -> {
            try {
                defaultSocketServer.start();
                startFuture.complete(null);
                defaultSocketServer.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                startFuture.completeExceptionally(e);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                startFuture.completeExceptionally(e);
            }
            countDownLatch.countDown();
        });
        startFuture.get();

        manager.start();
        for (ServerListener listener : serverListeners) {
            ClusterManager clusterManager = manager.getClusterManager();
            if (clusterManager != null) {
                Node thisNode = clusterManager.getThisNode();
                listener.onStartup(thisNode);
            }
        }
        countDownLatch.await();
    }

    public void shutdown() throws Exception {
        ClusterManager clusterManager = manager.getClusterManager();
        if (clusterManager != null) {
            Node thisNode = clusterManager.getThisNode();
            for (ServerListener listener : serverListeners) {
                listener.onShutdown(thisNode);
                if (listener instanceof Closeable) {
                    ((Closeable) listener).close();
                }
            }
        }
        manager.shutdown();
        defaultSocketServer.shutdown();
        if (!socketServerExecutor.isTerminated() || !socketServerExecutor.isShutdown()) {
            socketServerExecutor.shutdown();
            boolean shutdown = socketServerExecutor.awaitTermination(30, TimeUnit.SECONDS);
            if (logger.isInfoEnabled()) {
                logger.info(shutdown ? "All tasks was finished successfully" :
                        "Some tasks might still be running after 30 seconds");
            }
        }
    }
}
