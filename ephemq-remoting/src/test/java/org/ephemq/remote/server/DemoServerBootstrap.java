package org.ephemq.remote.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.codec.MessageDecoder;
import org.ephemq.remote.codec.MessageEncoder;
import org.ephemq.remote.handle.HeartbeatDuplexHandler;
import org.ephemq.remote.handle.ProcessDuplexHandler;
import org.ephemq.remote.invoke.Processor;
import org.ephemq.remote.util.NetworkUtil;

/**
 * The DemoServerBootstrap class is responsible for initializing and starting
 * a demo server using Netty framework. It configures the server's event loop
 * groups, sets various channel options, and initializes the channel pipeline
 * with handlers for packet encoding/decoding and processing.
 */
public class DemoServerBootstrap {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoServerBootstrap.class);

    /**
     * The main method is the entry point for the DemoServerBootstrap application.
     * It initializes and starts a server using the Netty framework, configures event loop groups,
     * sets channel options, and initializes the channel pipeline with necessary handlers.
     *
     * @param args Command-line arguments for the application (not used in this method).
     */
    public static void main(String[] args) {
        EventLoopGroup boosGroup = NetworkUtil.newEventLoopGroup(true, 1, "demo-server-boss", false, false);
        EventLoopGroup workerGroup = NetworkUtil.newEventLoopGroup(true, 0, "demo-server-worker", false, false);

        Processor processorAware = new DemoServerProcessor();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(boosGroup, workerGroup)
                    .channel(NetworkUtil.preferServerIoUringChannelClass(true, false))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, false)
                    .childOption(ChannelOption.SO_SNDBUF, 65536)
                    .childOption(ChannelOption.SO_RCVBUF, 65536);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    socketChannel.pipeline()
                            .addLast("packet-encoder", MessageEncoder.instance())
                            .addLast("paket-decoder", new MessageDecoder(0))
                            .addLast("connect-handler", new HeartbeatDuplexHandler(0, 30000))
                            .addLast("service-handler", new ProcessDuplexHandler(processorAware));
                }
            });

            Channel channel = serverBootstrap.bind(8888).sync().channel();
            if (logger.isInfoEnabled()) {
                logger.info("Demo start running, and listened at {}", 8888);
            }
            channel.closeFuture().sync();
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Demo started failed, {}", t);
            }
        } finally {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
