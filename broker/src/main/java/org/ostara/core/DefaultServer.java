package org.ostara.core;

import com.google.inject.Inject;
import org.ostara.common.Node;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.listener.ServerListener;
import org.ostara.management.ClusterManager;
import org.ostara.management.Manager;
import org.ostara.network.DefaultSocketServer;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class DefaultServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultServer.class);

    private final Config config;
    private final List<ServerListener> serverListeners = new LinkedList<>();
    private final CountDownLatch countDownLatch;
    private final DefaultSocketServer defaultSocketServer;
    private final Manager manager;

    @Inject
    public DefaultServer(Config config, DefaultSocketServer defaultSocketServer, Manager manager) {
        this.config = config;
        this.defaultSocketServer = defaultSocketServer;
        this.manager = manager;
        this.countDownLatch = new CountDownLatch(1);
    }

    public void addListener(ServerListener listener) {
        serverListeners.add(listener);
    }

    public void start() throws Exception {
        CompletableFuture<Void> startFuture = new CompletableFuture<>();
        new Thread(() -> {
            try {
                defaultSocketServer.start();
                startFuture.complete(null);
                defaultSocketServer.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                startFuture.completeExceptionally(e);
            } catch (Exception e) {
                startFuture.completeExceptionally(e);
                logger.error(e.getMessage(), e);
            }
            countDownLatch.countDown();
        }, "socket-server").start();

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
                    ((Closeable)listener).close();
                }
            }
        }

        manager.shutdown();
        defaultSocketServer.shutdown();
    }
}
