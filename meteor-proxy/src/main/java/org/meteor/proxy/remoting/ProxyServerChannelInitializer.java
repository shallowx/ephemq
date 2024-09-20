package org.meteor.proxy.remoting;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;
import org.meteor.remoting.ServiceChannelInitializer;
import org.meteor.remoting.ServiceDuplexHandler;
import org.meteor.support.Manager;

/**
 * ProxyServerChannelInitializer is responsible for initializing the channel for the proxy server.
 * It extends the ServiceChannelInitializer and configures the channel pipeline with the necessary
 * handlers for the proxy operation.
 */
public class ProxyServerChannelInitializer extends ServiceChannelInitializer {
    /**
     * Holds the proxy server configuration utilized by ProxyServerChannelInitializer for setting up the proxy server.
     * The configuration encapsulates various parameters and properties essential for initializing and managing
     * the proxy server, including but not limited to network settings, common configurations, and proxy-specific
     * settings derived from the overarching server configuration.
     */
    private final ProxyServerConfig serverConfiguration;

    /**
     * Creates an instance of ProxyServerChannelInitializer, which initializes the channel for the proxy server.
     * It configures the channel with the necessary settings from the provided server configuration and manager.
     *
     * @param serverConfiguration The configuration settings specific to the proxy server, including common and network configs.
     * @param manager             The manager responsible for overseeing channel operations and the interaction of various server components.
     */
    public ProxyServerChannelInitializer(ProxyServerConfig serverConfiguration, Manager manager) {
        super(serverConfiguration.getCommonConfig(), serverConfiguration.getNetworkConfig(), manager);
        this.serverConfiguration = serverConfiguration;
    }

    /**
     * Initializes the given {@code SocketChannel} by adding the necessary handlers to its {@code ChannelPipeline}.
     * This method configures various handlers such as logging, encoding, decoding, and processing handlers.
     *
     * @param socketChannel the {@code SocketChannel} to initialize
     */
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
        pipeline.addLast("proxy-processor-handler", new ServiceDuplexHandler(
                manager, new ProxyServiceProcessor(serverConfiguration, manager)));
    }
}
