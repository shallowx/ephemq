package org.leopard.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

import static org.leopard.remote.util.NetworkUtils.newEventLoopGroup;
import static org.leopard.remote.util.NetworkUtils.preferServerChannelClass;

public final class SimpleSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SimpleSocketServer.class);

    private final ServerConfig config;
    private final ServerChannelInitializer serverChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workGroup;
    private ChannelFuture closedFuture;

    public SimpleSocketServer(ServerConfig config, ResourceContext manager) {
        this.config = config;
        this.serverChannelInitializer = new ServerChannelInitializer(config, manager);
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

        WriteBufferWaterMark mark = new WriteBufferWaterMark(config.getSocketWriteHighWaterMark() >> 1, config.getSocketWriteHighWaterMark());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind(config.getExposedHost(), config.getExposedPort())
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess() && logger.isInfoEnabled()) {
                        logger.info("Socket server is listening at {}", f.channel().localAddress());
                    } else {
                        if (logger.isErrorEnabled()) {
                            logger.error("Socket server start failed", f.cause());
                        }
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
