package org.ostara.remote.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.processor.ProcessorAware;
import org.ostara.remote.util.NetworkUtils;

public class DemoServerBootstrap {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoServerBootstrap.class);
    public static void main(String[] args) {
        EventLoopGroup boosGroup = NetworkUtils.newEventLoopGroup(true, 1, "demo-server-boss");
        EventLoopGroup workerGroup = NetworkUtils.newEventLoopGroup(true, 0, "demo-server-worker");
        EventExecutorGroup servicesGroup = NetworkUtils.newEventExecutorGroup(0, "demo-server-service");

        ProcessorAware processorAware = new DemoServerProcessor();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(boosGroup, workerGroup)
                    .channel(NetworkUtils.preferServerChannelClass(true))
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, false)
                    .childOption(ChannelOption.SO_SNDBUF, 65536)
                    .childOption(ChannelOption.SO_RCVBUF, 65536);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline()
                            .addLast(servicesGroup, "packet-encoder", MessageEncoder.instance())
                            .addLast(servicesGroup, "paket-decoder", new MessageDecoder())
                            .addLast(servicesGroup, "connect-handler", new ConnectDuplexHandler(0, 30000))
                            .addLast(servicesGroup, "service-handler", new ProcessDuplexHandler(processorAware));
                }
            });

            Channel channel = serverBootstrap.bind(8888).sync().channel();
            logger.info("Demo start running, and listened at {}", 8888);
            channel.closeFuture().sync();
        } catch (Throwable t){
            logger.error("Demo started failed");
        } finally {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            servicesGroup.shutdownGracefully();
        }
    }
}
