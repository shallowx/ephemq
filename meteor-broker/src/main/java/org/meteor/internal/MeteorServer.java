package org.meteor.internal;

import io.netty.util.concurrent.DefaultThreadFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.listener.ServerListener;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.ClusterCoordinator;
import org.meteor.support.Coordinator;

public class MeteorServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorServer.class);
    private static final ExecutorService socketServerExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("socket-server"));
    private final List<ServerListener> serverListeners = new ObjectArrayList<>();
    private final CountDownLatch countDownLatch;
    private final DefaultSocketServer defaultSocketServer;
    private final Coordinator coordinator;

    public MeteorServer(DefaultSocketServer defaultSocketServer, Coordinator coordinator) {
        this.defaultSocketServer = defaultSocketServer;
        this.coordinator = coordinator;
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

        coordinator.start();
        for (ServerListener listener : serverListeners) {
            ClusterCoordinator clusterCoordinator = coordinator.getClusterCoordinator();
            if (clusterCoordinator != null) {
                Node thisNode = clusterCoordinator.getThisNode();
                listener.onStartup(thisNode);
            }
        }
        countDownLatch.await();
    }

    public void shutdown() throws Exception {
        ClusterCoordinator clusterCoordinator = coordinator.getClusterCoordinator();
        if (clusterCoordinator != null) {
            Node thisNode = clusterCoordinator.getThisNode();
            for (ServerListener listener : serverListeners) {
                listener.onShutdown(thisNode);
                if (listener instanceof Closeable) {
                    ((Closeable) listener).close();
                }
            }
        }
        coordinator.shutdown();
        defaultSocketServer.shutdown();
        if (!socketServerExecutor.isTerminated() || !socketServerExecutor.isShutdown()) {
            socketServerExecutor.shutdown();
        }
    }
}
