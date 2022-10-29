package org.shallow.nameserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

import static org.shallow.remote.util.NetworkUtil.newEventLoopGroup;
import static org.shallow.remote.util.NetworkUtil.preferServerChannelClass;

public final class NameserverSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(NameserverSocketServer.class);

    private final ServerChannelInitializer serverChannelInitializer;
    private  EventLoopGroup bossGroup;
    private  EventLoopGroup workGroup;
    private  ChannelFuture closedFuture;

    public NameserverSocketServer() {
        this.serverChannelInitializer = new ServerChannelInitializer();
    }

    public void start() throws Exception {
        bossGroup = newEventLoopGroup(true, 1, "server-acceptor");
        workGroup = newEventLoopGroup(true, 16, "server-processor");

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(preferServerChannelClass(true))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.SO_SNDBUF, 65536)
                .childOption(ChannelOption.SO_RCVBUF, 65536)
                .childHandler(serverChannelInitializer);

        WriteBufferWaterMark mark = new WriteBufferWaterMark(1024 >> 1 , 1024);
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, mark);

        ChannelFuture future = bootstrap.bind("", 9000)
                .addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess() && logger.isInfoEnabled()) {
                        logger.info("Socket server is listening at {}", f.channel().localAddress());
                    } else  {
                        if (logger.isErrorEnabled()) {
                            logger.error("Socket server start failed", f.cause());
                        }
                    }
                }).sync();

        closedFuture = future.channel().closeFuture();
    }

    public void awaitShutdownGracefully(){
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
