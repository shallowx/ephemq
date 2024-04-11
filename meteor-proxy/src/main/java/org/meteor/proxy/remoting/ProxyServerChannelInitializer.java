package org.meteor.proxy.remoting;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;
import org.meteor.remoting.ServiceChannelInitializer;
import org.meteor.remoting.ServiceDuplexHandler;
import org.meteor.support.Manager;

public class ProxyServerChannelInitializer extends ServiceChannelInitializer {
    private final ProxyServerConfig serverConfiguration;

    public ProxyServerChannelInitializer(ProxyServerConfig serverConfiguration, Manager coordinator) {
        super(serverConfiguration.getCommonConfig(), serverConfiguration.getNetworkConfig(), coordinator);
        this.serverConfiguration = serverConfiguration;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (networkConfiguration.isNetworkLogDebugEnabled()) {
            pipeline.addLast("proxy-logging-handler", new LoggingHandler(LogLevel.DEBUG));
        }

        pipeline.addLast("proxy-statistics-handler", statisticsDuplexHandler);
        pipeline.addLast("proxy-encoder", MessageEncoder.instance());
        pipeline.addLast("proxy-decoder", new MessageDecoder());
        pipeline.addLast("proxy-connect-handler", new HeartbeatDuplexHandler(0, 60000));
        pipeline.addLast("proxy-processor-handler", new ServiceDuplexHandler(coordinator, new ProxyServiceProcessor(serverConfiguration, coordinator)));
    }
}
