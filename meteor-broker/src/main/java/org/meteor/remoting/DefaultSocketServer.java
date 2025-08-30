package org.meteor.remoting;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.CommonConfig;
import org.meteor.config.NetworkConfig;
import org.meteor.config.ServerConfig;
import org.meteor.support.Manager;

import java.net.SocketAddress;

import static org.meteor.metrics.config.MetricsConstants.*;
import static org.meteor.remote.util.NetworkUtil.newEventLoopGroup;
import static org.meteor.remote.util.NetworkUtil.preferServerIoUringChannelClass;

/**
 * DefaultSocketServer is responsible for initializing and starting a socket server using the provided configuration.
 * This class handles the setup of Netty event loop groups, server bootstrap configuration, and socket binding.
 * <p>
 * It manages the lifecycle of the server, including starting, waiting for shutdown, and shutting down gracefully.
 */
public class DefaultSocketServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultSocketServer.class);
    /**
     * Immutable common configuration settings for the DefaultSocketServer.
     * <p>
     * This variable holds an instance of CommonConfig, which provides access
     * to various server configuration parameters such as server IDs, cluster
     * names, advertised addresses, ports, and thread settings.
     * <p>
     * The configuration settings are retrieved and used throughout the
     * DefaultSocketServer class to ensure consistent and centralized
     * configuration management.
     */
    private final CommonConfig commonConfiguration;
    /**
     * Holds the network configuration settings for the {@code DefaultSocketServer}.
     * This variable is an instance of {@code NetworkConfig} and encapsulates various
     * network-related properties such as connection timeouts, write buffer sizes, and thread limits.
     */
    private final NetworkConfig networkConfiguration;
    /**
     * Initializes the service channel pipeline with handlers for
     * logging, statistics, encoding/decoding, heartbeat management,
     * and request processing. This initializer is configured with
     * the necessary settings and components to handle incoming
     * socket channels within the server context.
     */
    protected ServiceChannelInitializer serviceChannelInitializer;
    /**
     * The EventLoopGroup responsible for accepting connections in the DefaultSocketServer.
     * This group is typically used to handle incoming connection requests.
     */
    private EventLoopGroup bossGroup;
    /**
     * Represents the worker group for handling I/O operations.
     * <p>
     * This is used in the context of a {@link DefaultSocketServer} to manage
     * the event loop for processing network events.
     */
    private EventLoopGroup workGroup;
    /**
     * A {@link ChannelFuture} that represents the asynchronous result of the closing of the service channel.
     * <p>
     * This future is used to track the status of the channel closing operation and can be used to perform actions
     * once the channel is successfully closed or if it fails to close.
     */
    private ChannelFuture channelClosedFuture;

    /**
     * Constructs a DefaultSocketServer instance with the specified server configuration and manager.
     *
     * @param serverConfiguration The configuration settings for the server, encapsulated in a ServerConfig object.
     * @param manager The Manager instance responsible for managing server-related operations and tasks.
     */
    public DefaultSocketServer(ServerConfig serverConfiguration, Manager manager) {
        this.commonConfiguration = serverConfiguration.getCommonConfig();
        this.networkConfiguration = serverConfiguration.getNetworkConfig();
        this.serviceChannelInitializer =
                new ServiceChannelInitializer(commonConfiguration, networkConfiguration, manager);
    }

    /**
     * Starts the DefaultSocketServer by initializing thread groups and configuring the server.
     * This method sets up the necessary event loop groups for accepting and processing
     * network connections using Netty's `ServerBootstrap`.
     * <p>
     * It also binds the server to the advertised address and port specified in the configuration.
     * If a compatible port is available and differs from the advertised port, the server
     * will listen on that port as well.
     * <p>
     * The method may throw an exception if the server fails to start.
     *
     * @throws Exception if there is an error during server startup
     */
    public void start() throws Exception {
        bossGroup = newEventLoopGroup(commonConfiguration.isSocketPreferEpoll(), networkConfiguration.getIoThreadLimit(), "server-acceptor", false, commonConfiguration.isSocketPreferIoUring());
        gauge(bossGroup, "acceptor");

        workGroup = newEventLoopGroup(commonConfiguration.isSocketPreferEpoll(), networkConfiguration.getNetworkThreadLimit(), "server-processor",
                commonConfiguration.isThreadAffinityEnabled(), commonConfiguration.isSocketPreferIoUring());
        gauge(workGroup, "processor");

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(preferServerIoUringChannelClass(commonConfiguration.isSocketPreferEpoll(), commonConfiguration.isSocketPreferIoUring()))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.SO_SNDBUF, 65536)
                .childOption(ChannelOption.SO_RCVBUF, 65536)
                .childHandler(serviceChannelInitializer);
        if (networkConfiguration.isNetworkLogDebugEnabled()) {
            bootstrap.handler(new LoggingHandler(LogLevel.DEBUG));
        }

        WriteBufferWaterMark mark = new WriteBufferWaterMark(networkConfiguration.getWriteBufferWaterMarker() >> 1,
                networkConfiguration.getWriteBufferWaterMarker());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);
        ChannelFuture future = bootstrap.bind(commonConfiguration.getAdvertisedAddress(), commonConfiguration.getAdvertisedPort())
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Socket server is listening at socket address[{}]", f.channel().localAddress());
                        }
                    } else {
                        if (logger.isErrorEnabled()) {
                            logger.error("Socket server start failed", f.cause());
                        }
                    }
                }).sync();

        channelClosedFuture = future.channel().closeFuture();
        int compatiblePort = commonConfiguration.getCompatiblePort();
        if (future.isSuccess() && compatiblePort >= 0 && compatiblePort != commonConfiguration.getAdvertisedPort()) {
            Channel compatibleChannel = bootstrap.bind(commonConfiguration.getAdvertisedAddress(), compatiblePort)
                    .addListener((ChannelFutureListener) cf -> {
                        if (cf.isSuccess()) {
                            if (logger.isInfoEnabled()) {
                                SocketAddress address = cf.channel().localAddress();
                                logger.info("Socket compatible server is listening at socket address[{}]", address);
                            }
                        } else {
                            if (logger.isErrorEnabled()) {
                                logger.error("Socket compatible server start failed", cf.cause());
                            }
                        }
                    }).channel();
            channelClosedFuture.addListener(f -> compatibleChannel.close());
        }
    }

    /**
     * Registers a set of gauges to monitor the pending tasks for each event executor within an EventLoopGroup.
     *
     * @param workGroup The EventLoopGroup which contains the executors to be monitored.
     * @param processor An identifier for the type of processor being used for tagging the metrics.
     */
    private void gauge(EventLoopGroup workGroup, String processor) {
        for (EventExecutor executor : workGroup) {
            SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
            Gauge.builder(NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, commonConfiguration.getClusterName())
                    .tag(BROKER_TAG, commonConfiguration.getServerId())
                    .tag(TYPE_TAG, processor)
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", singleThreadEventExecutor.threadProperties().name()).register(Metrics.globalRegistry);
        }
    }

    /**
     * Waits for the channel to close synchronously, blocking until the channel closure is complete.
     * This method helps ensure the orderly shutdown of the socket server by awaiting the closure
     * of the associated channel.
     * <p>
     * Any interruption during the wait is caught and logged as an error.
     */
    public void awaitShutdown() {
        try {
            channelClosedFuture.sync();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Shuts down the DefaultSocketServer and its associated resources gracefully.
     *
     * @throws Exception if an error occurs during the shutdown process.
     * <p>
     * This method attempts to close the channel first, making sure all operations
     * are synchronized. It then gracefully shuts down the boss and worker groups,
     * ensuring all tasks are completed before termination. If any exceptions occur
     * during these shutdown processes, the error is logged.
     */
    public void shutdown() throws Exception {
        if (channelClosedFuture != null) {
            try {
                channelClosedFuture.channel().close().sync();
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        if (bossGroup != null) {
            try {
                bossGroup.shutdownGracefully().sync();
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        if (workGroup != null) {
            try {
                workGroup.shutdownGracefully().sync();
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
