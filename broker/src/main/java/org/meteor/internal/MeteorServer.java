package org.meteor.internal;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.meteor.listener.ServerListener;
import org.meteor.coordinatio.ClusterCoordinator;
import org.meteor.coordinatio.Coordinator;
import org.meteor.net.DefaultSocketServer;
import org.meteor.common.Node;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MeteorServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorServer.class);
    private final List<ServerListener> serverListeners = new LinkedList<>();
    private final CountDownLatch countDownLatch;
    private final DefaultSocketServer defaultSocketServer;
    private final Coordinator manager;
    private static final ExecutorService socketServerExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("socket-server"));

    public MeteorServer(DefaultSocketServer defaultSocketServer, Coordinator manager) {
        this.defaultSocketServer = defaultSocketServer;
        this.manager = manager;
        this.countDownLatch = new CountDownLatch(1);
    }

    public void addListener(ServerListener listener) {
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
            ClusterCoordinator clusterManager = manager.getClusterManager();
            if (clusterManager != null) {
                Node thisNode = clusterManager.getThisNode();
                listener.onStartup(thisNode);
            }
        }
        countDownLatch.await();
    }

    public void shutdown() throws Exception {
        ClusterCoordinator clusterManager = manager.getClusterManager();
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
        }
    }
}
