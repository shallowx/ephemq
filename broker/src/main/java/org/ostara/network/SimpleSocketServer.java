package org.ostara.network;

import static org.ostara.remote.util.NetworkUtils.newEventLoopGroup;
import static org.ostara.remote.util.NetworkUtils.preferServerChannelClass;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.context.ResourceContext;
import org.ostara.config.ServerConfig;

public final class SimpleSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SimpleSocketServer.class);
    private final ServerConfig config;
    private final ServerChannelInitializer serverChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;
    private ChannelFuture closedFuture;

    public SimpleSocketServer(ServerConfig config, ResourceContext context) {
        this.config = config;
        this.serverChannelInitializer = new ServerChannelInitializer(config, context);
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.getIoThreadLimit(), "server-acceptor");
        workGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.getNetworkThreadLimit(), "server-processor");

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(preferServerChannelClass(config.isOsEpollPrefer()))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.SO_SNDBUF, 65536)
                .childOption(ChannelOption.SO_RCVBUF, 65536)
                .childHandler(serverChannelInitializer);

        if (config.isNetworkLoggingDebugEnabled()) {
            bootstrap.handler(new LoggingHandler(LogLevel.DEBUG));
        }

        WriteBufferWaterMark mark = new WriteBufferWaterMark(config.getSocketWriteHighWaterMark() >> 1,
                config.getSocketWriteHighWaterMark());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind(config.getExposedHost(), config.getExposedPort())
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess() && logger.isInfoEnabled()) {
                        logger.info("Socket server is listening at {}", f.channel().localAddress());
                    } else {
                        logger.error("Socket server start failed", f.cause());
                    }
                }).sync();

        closedFuture = future.channel().closeFuture();
    }

    public void awaitShutdownGracefully() {
        try {
            closedFuture.sync();
        } catch (InterruptedException e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void shutdownGracefully() throws Exception {
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
