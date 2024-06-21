package org.meteor.remoting;

import static org.meteor.metrics.config.MetricsConstants.BROKER_TAG;
import static org.meteor.metrics.config.MetricsConstants.CLUSTER_TAG;
import static org.meteor.metrics.config.MetricsConstants.NETTY_PENDING_TASK_NAME;
import static org.meteor.metrics.config.MetricsConstants.TYPE_TAG;
import static org.meteor.remote.util.NetworkUtil.newEventLoopGroup;
import static org.meteor.remote.util.NetworkUtil.preferServerChannelClass;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.CommonConfig;
import org.meteor.config.NetworkConfig;
import org.meteor.config.ServerConfig;
import org.meteor.support.Manager;

public class DefaultSocketServer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultSocketServer.class);
    private final CommonConfig commonConfiguration;
    private final NetworkConfig networkConfiguration;
    protected ServiceChannelInitializer serviceChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;
    private ChannelFuture channelClosedFuture;

    public DefaultSocketServer(ServerConfig serverConfiguration, Manager manager) {
        this.commonConfiguration = serverConfiguration.getCommonConfig();
        this.networkConfiguration = serverConfiguration.getNetworkConfig();
        this.serviceChannelInitializer =
                new ServiceChannelInitializer(commonConfiguration, networkConfiguration, manager);
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(true, networkConfiguration.getIoThreadLimit(), "server-acceptor", false);
        gauge(bossGroup, "acceptor");

        workGroup = newEventLoopGroup(true, networkConfiguration.getNetworkThreadLimit(), "server-processor",
                commonConfiguration.isThreadAffinityEnabled());
        gauge(workGroup, "processor");

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(preferServerChannelClass(commonConfiguration.isSocketPreferEpoll()))
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

    public void awaitShutdown() {
        try {
            channelClosedFuture.sync();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

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
