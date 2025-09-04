package org.ephemq.remoting;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.ephemq.config.CommonConfig;
import org.ephemq.config.NetworkConfig;
import org.ephemq.remote.codec.MessageDecoder;
import org.ephemq.remote.codec.MessageEncoder;
import org.ephemq.remote.handle.HeartbeatDuplexHandler;
import org.ephemq.support.Manager;

/**
 * ServiceChannelInitializer is a custom ChannelInitializer for setting up the pipeline of a SocketChannel.
 * It utilizes the provided configurations and manager to initialize various handlers needed for
 * the service operations.
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {
    /**
     * Holds the common configuration settings required for initializing and managing
     * various service operations. It encapsulates configuration parameters shared
     * across different components in the system to ensure consistency and centralized
     * management of these settings.
     */
    protected final CommonConfig commonConfiguration;
    /**
     * Holds the network-related configuration properties. This configuration is used for
     * setting up various network parameters, such as connection timeouts, buffer sizes,
     * thread limits, and logging preferences.
     */
    protected final NetworkConfig networkConfiguration;
    /**
     * The manager instance responsible for handling the core functionalities required for the proper
     * initialization and operation of the service. This variable is used in conjunction with
     * other components to set up the necessary handlers and processors in the channel pipeline.
     */
    protected final Manager manager;
    /**
     * The StatisticsDuplexHandler is responsible for handling statistics related to the number of active channels.
     * It extends ChannelDuplexHandler and overrides the channelActive and channelInactive methods
     * to update the count of active channels.
     */
    protected final StatisticsDuplexHandler statisticsDuplexHandler;

    /**
     * Initializes the ServiceChannelInitializer with the specified configurations and manager.
     * This constructor sets up the initial state required for the channel initializer to function,
     * including common configuration, network configuration, and a manager instance.
     *
     * @param commonConfiguration  The common configuration settings for the service.
     * @param networkConfiguration The network configuration settings, such as port and address.
     * @param manager              The manager instance responsible for overseeing channel operations.
     */
    public ServiceChannelInitializer(CommonConfig commonConfiguration, NetworkConfig networkConfiguration,
                                     Manager manager) {
        this.commonConfiguration = commonConfiguration;
        this.networkConfiguration = networkConfiguration;
        this.manager = manager;
        this.statisticsDuplexHandler = new StatisticsDuplexHandler(commonConfiguration);
    }

    /**
     * Initializes the channel by setting up the necessary handlers in the pipeline.
     *
     * @param socketChannel the {@link SocketChannel} to be initialized
     * @throws Exception if any error occurs during channel initialization
     */
    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (networkConfiguration.isNetworkLogDebugEnabled()) {
            pipeline.addLast("logging-handler", new LoggingHandler(LogLevel.DEBUG));
        }
        pipeline.addLast("statistics-handler", statisticsDuplexHandler);
        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder(commonConfiguration.getDiscardAfterReads()));
        pipeline.addLast("connect-handler", new HeartbeatDuplexHandler(0, 60000));
        pipeline.addLast("processor-handler", new ServiceDuplexHandler(manager,
                new ServiceProcessor(commonConfiguration, networkConfiguration, manager)));
    }
}
