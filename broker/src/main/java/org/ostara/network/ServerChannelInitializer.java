package org.ostara.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.config.ServerConfig;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ServerConfig config;
    private final ResourceContext manager;

    public ServerChannelInitializer(ServerConfig config, ResourceContext manager) {
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
        pipeline.addLast("processor-handler", new ServiceDuplexHandler(new MessageProcessorAware(config, manager)));
    }
}
