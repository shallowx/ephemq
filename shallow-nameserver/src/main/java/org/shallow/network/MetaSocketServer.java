package org.shallow.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.shallow.internal.MetaConfig;
import org.shallow.internal.MetaManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import static org.shallow.util.NetworkUtil.newEventLoopGroup;
import static org.shallow.util.NetworkUtil.preferServerChannelClass;

public class MetaSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetaSocketServer.class);

    private final MetaConfig config;
    private final MetaServerChannelInitializer serverChannelInitializer;
    private EventLoopGroup bossGroup;
    private  EventLoopGroup workGroup;
    private ChannelFuture closedFuture;
    private final MetaManager manager;

    public MetaSocketServer(MetaConfig config, MetaManager manager) {
        this.config = config;
        this.manager = manager;
        this.serverChannelInitializer = new MetaServerChannelInitializer(config, manager);
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.getIoThreadWholes(), "server-acceptor");
        workGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.getNetworkThreadWholes(), "server-processor");

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

        WriteBufferWaterMark mark = new WriteBufferWaterMark(config.getSocketWriteHighWaterMark() >> 1 , config.getSocketWriteHighWaterMark());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind(config.getExposedHost(), config.getExposedPort())
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess() && logger.isInfoEnabled()) {
                        logger.info("Socket server is listening at {}", f.channel().localAddress());
                    } else  {
                        if (logger.isErrorEnabled()) {
                            logger.info("Socket server start failed", f.cause());
                        }
                    }
                }).sync();

        closedFuture = future.channel().closeFuture();
    }

    public void awaitShutdownGracefully() throws InterruptedException {
        closedFuture.sync();
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
