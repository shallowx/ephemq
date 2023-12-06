package org.ostara.beans;

import com.google.inject.Inject;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.ostara.common.Node;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.listener.ServerListener;
import org.ostara.management.ClusterManager;
import org.ostara.management.Manager;
import org.ostara.net.CoreSocketServer;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OstaraServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(OstaraServer.class);
    private final CoreConfig config;
    private final List<ServerListener> serverListeners = new LinkedList<>();
    private final CountDownLatch countDownLatch;
    private final CoreSocketServer defaultSocketServer;
    private final Manager manager;
    private static final ExecutorService socketServerExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("socket-server"));
    @Inject
    public OstaraServer(CoreConfig config, CoreSocketServer defaultSocketServer, Manager manager) {
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
        }
    }
}
