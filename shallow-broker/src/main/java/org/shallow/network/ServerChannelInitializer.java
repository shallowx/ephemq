package org.shallow.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.shallow.internal.BrokerConfig;
import org.shallow.codec.MessageDecoder;
import org.shallow.codec.MessageEncoder;
import org.shallow.internal.BrokerManager;
import org.shallow.handle.ConnectDuplexHandler;

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
        pipeline.addLast("service-handler", new ServiceDuplexHandler(new BrokerProcessorAware(manager)));
    }
}
