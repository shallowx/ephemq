package org.meteor.proxy.net;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.meteor.configuration.ServerConfiguration;
import org.meteor.management.Manager;
import org.meteor.net.ServiceChannelInitializer;
import org.meteor.net.ServiceDuplexHandler;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;

public class ProxyServerChannelInitializer extends ServiceChannelInitializer {
    private final ServerConfiguration serverConfiguration;
    public ProxyServerChannelInitializer(ServerConfiguration serverConfiguration, Manager manager) {
        super(serverConfiguration.getCommonConfiguration(), serverConfiguration.getNetworkConfiguration(), manager);
        this.serverConfiguration = serverConfiguration;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (networkConfiguration.isNetworkLogDebugEnabled()) {
            pipeline.addLast("logging-handler", new LoggingHandler(LogLevel.DEBUG));
        }

        pipeline.addLast("statistics-handler", statisticsDuplexHandler);
        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("connect-handler", new HeartbeatDuplexHandler(0, 60000));
        pipeline.addLast("processor-handler", new ServiceDuplexHandler(manager, new ProxyServiceProcessor(serverConfiguration, manager)));
    }
}
