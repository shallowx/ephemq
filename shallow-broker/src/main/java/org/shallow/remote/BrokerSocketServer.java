package org.shallow.remote;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.shallow.internal.BrokerConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import static org.shallow.util.NetworkUtil.newEventLoopGroup;
import static org.shallow.util.NetworkUtil.preferServerChannelClass;

public final class BrokerSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerConfig config;
    private final ServerChannelInitializer serverChannelInitializer;
    private  EventLoopGroup bossGroup;
    private  EventLoopGroup workGroup;
    private  ChannelFuture closedFuture;
    private final BrokerManager manager;

    public BrokerSocketServer(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;
        this.serverChannelInitializer = new ServerChannelInitializer(config, manager);
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.obtainIoThreadWholes(), "server-acceptor");
        workGroup = newEventLoopGroup(config.isOsEpollPrefer(), config.obtainNetworkThreadWholes(), "server-processor");

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

        WriteBufferWaterMark mark = new WriteBufferWaterMark(config.obtainSocketWriteHighWaterMark() >> 1 , config.obtainSocketWriteHighWaterMark());
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind(config.obtainExposedHost(), config.obtainExposedPort())
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
