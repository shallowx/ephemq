package org.meteor.net;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.meteor.configuration.CommonConfiguration;
import org.meteor.configuration.NetworkConfiguration;
import org.meteor.management.Manager;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;
import org.meteor.remote.processor.Processor;

public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    protected final CommonConfiguration commonConfiguration;
    protected final NetworkConfiguration networkConfiguration;
    protected final Manager manager;
    protected final StatisticsDuplexHandler statisticsDuplexHandler;

    public ServiceChannelInitializer(CommonConfiguration commonConfiguration, NetworkConfiguration networkConfiguration, Manager manager) {
        this.commonConfiguration = commonConfiguration;
        this.networkConfiguration = networkConfiguration;
        this.manager = manager;
        this.statisticsDuplexHandler = new StatisticsDuplexHandler(commonConfiguration);
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
        pipeline.addLast("processor-handler", new ServiceDuplexHandler(manager, new ServiceProcessor(commonConfiguration, networkConfiguration, manager)));
    }
}
