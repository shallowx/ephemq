package org.ostara.network;

import com.google.inject.Inject;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.management.Manager;

import java.net.SocketAddress;

import static org.ostara.metrics.MetricsConstants.*;
import static org.ostara.remote.util.NetworkUtils.newEventLoopGroup;
import static org.ostara.remote.util.NetworkUtils.preferServerChannelClass;
public class DefaultSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultSocketServer.class);
    private final Config config;
    private ServiceChannelInitializer serviceChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;
    private ChannelFuture closedFuture;

    @Inject
    public DefaultSocketServer(Config config, Manager manager) {
        this.config = config;
    }

    @Inject
    public void setServiceChannelInitializer(ServiceChannelInitializer serviceChannelInitializer) {
        this.serviceChannelInitializer = serviceChannelInitializer;
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(true, config.getIoThreadCounts(), "server-acceptor");
        for (EventExecutor executor : bossGroup) {
            SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
            Gauge.builder(NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "acceptor")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", singleThreadEventExecutor.threadProperties().name()).register(Metrics.globalRegistry);
        }

        workGroup = newEventLoopGroup(true, config.getNetworkThreadCounts(), "server-processor");
        for (EventExecutor executor : workGroup) {
            SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
            Gauge.builder(NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "processor")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", singleThreadEventExecutor.threadProperties().name()).register(Metrics.globalRegistry);
        }

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(preferServerChannelClass(true))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.SO_SNDBUF, 65536)
                .childOption(ChannelOption.SO_RCVBUF, 65536)
                .childHandler(serviceChannelInitializer);

        if (config.isNetworkLogDebugEnabled()) {
            bootstrap.handler(new LoggingHandler(LogLevel.DEBUG));
        }

        WriteBufferWaterMark mark = new WriteBufferWaterMark(config.getWriteBufferWaterMarker() >> 1,
                config.getWriteBufferWaterMarker());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind(config.getAdvertisedAddress(), config.getAdvertisedPort())
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess() && logger.isInfoEnabled()) {
                        logger.info("Socket server is listening at {}", f.channel().localAddress());
                    } else {
                        logger.error("Socket server start failed", f.cause());
                    }
                }).sync();

        closedFuture = future.channel().closeFuture();

        int compatiblePort = config.getCompatiblePort();
        if (future.isSuccess() && compatiblePort >= 0 && compatiblePort != config.getAdvertisedPort()) {
            Channel compatibleChannel = bootstrap.bind(config.getAdvertisedAddress(), compatiblePort)
                    .addListener((ChannelFutureListener) cf -> {
                        if (cf.isSuccess()) {
                            SocketAddress address = cf.channel().localAddress();
                            logger.info("Socket compatible server is listening at {}", address);
                        } else {
                            logger.error("Socket server start failed", cf.cause());
                        }
                    }).channel();
            closedFuture.addListener(f -> compatibleChannel.close());
        }
    }

    public void awaitShutdown() {
        try {
            closedFuture.sync();
        } catch (InterruptedException e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void shutdown() throws Exception {
        if (closedFuture != null) {
            closedFuture.channel().close().sync();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
        }

        if (workGroup != null) {
            workGroup.shutdownGracefully().sync();
        }
    }
}
