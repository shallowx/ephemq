package org.leopard.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.leopard.remote.handle.ConnectDuplexHandler;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.remote.codec.MessageDecoder;
import org.leopard.remote.codec.MessageEncoder;
import org.leopard.internal.BrokerManager;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final BrokerConfig config;
    private final BrokerManager manager;

    public ServerChannelInitializer(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (config.isNetworkLoggingDebugEnabled()) {
            pipeline.addLast("logging-handler", new LoggingHandler(LogLevel.DEBUG));
        }

        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("connect-handler", new ConnectDuplexHandler(0, 60000));
        pipeline.addLast("service-handler", new ServiceDuplexHandler(new BrokerProcessorAware(config, manager)));
    }
}
