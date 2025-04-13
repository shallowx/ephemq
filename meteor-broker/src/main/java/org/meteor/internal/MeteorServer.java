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
import org.meteor.zookeeper.ClusterManager;
import org.meteor.support.Manager;

/**
 * The MeteorServer class manages the lifecycle of a socket server, including startup and shutdown.
 * It also handles server listeners for startup and shutdown events.
 */
public class MeteorServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorServer.class);
    /**
     * Executor service responsible for managing threads in the socket server.
     * This executor utilizes virtual threads, labeled with the prefix "socket-server",
     * to efficiently handle concurrent socket server tasks.
     */
    private static final ExecutorService socketServerExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("socket-server").factory()
    );
    /**
     * A list containing server listeners that respond to server events such as startup and shutdown.
     * This list is used by the MeteorServer class to manage and notify listeners of important server lifecycle events.
     */
    private final List<ServerListener> serverListeners = new ObjectArrayList<>();
    /**
     * A synchronization aid that allows one or more threads to wait until a set of operations
     * being performed in other threads completes. This is particularly useful in a server setting
     * where the server needs to ensure certain operations have finished before proceeding.
     * <p>
     * In the context of {@code MeteorServer}, this variable helps to manage synchronization
     * during the startup and shutdown processes.
     */
    private final CountDownLatch countDownLatch;
    /**
     * The `defaultSocketServer` is an instance of `DefaultSocketServer` that is used to manage socket server operations.
     * It initializes and binds the server to the specified address and port, and handles server startup and shutdown.
     * This instance is utilized within the `MeteorServer` class to control the server lifecycle, including starting and
     * shutting down the socket server seamlessly.
     */
    private final DefaultSocketServer defaultSocketServer;
    /**
     * The manager responsible for controlling various operations and services
     * within the MeteorServer, including startup, shutdown, and accessing
     * several executor groups and listeners associated with the system.
     */
    private final Manager manager;

    /**
     * Constructs a new MeteorServer with the specified DefaultSocketServer and Manager.
     *
     * @param defaultSocketServer the DefaultSocketServer to be used by the MeteorServer
     * @param manager the Manager to manage and coordinate the server's operations
     */
    public MeteorServer(DefaultSocketServer defaultSocketServer, Manager manager) {
        this.defaultSocketServer = defaultSocketServer;
        this.manager = manager;
        this.countDownLatch = new CountDownLatch(1);
    }

    /**
     * Adds a listener to the server, enabling it to handle server lifecycle events such as startup and shutdown.
     *
     * @param listener the ServerListener to be added.
     */
    public void addListener(@Nonnull ServerListener listener) {
        serverListeners.add(listener);
    }

    /**
     * Starts the MeteorServer, initializing the default socket server and the manager.
     * The method also notifies all registered server listeners about the startup.
     *
     * @throws Exception if any error occurs during the server startup.
     */
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

    /**
     * Shuts down the server and its associated resources.
     *
     * @throws Exception if an error occurs during the shutdown process
     */
    public void shutdown() throws Exception {
        ClusterManager clusterManager = manager.getClusterManager();
        if (clusterManager != null) {
            Node thisNode = clusterManager.getThisNode();
            for (ServerListener listener : serverListeners) {
                listener.onShutdown(thisNode);
                if (listener instanceof Closeable closeable) {
                    closeable.close();
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
