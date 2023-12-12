package org.meteor.net;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.meteor.core.CoreConfig;
import org.meteor.core.InjectBeans;
import org.meteor.management.Manager;
import org.meteor.remote.codec.MessageDecoder;
import org.meteor.remote.codec.MessageEncoder;
import org.meteor.remote.handle.HeartbeatDuplexHandler;

public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final CoreConfig config;
    private final Manager manager;
    private final StatisticsDuplexHandler statisticsDuplexHandler;

    @Inject
    public ServiceChannelInitializer(CoreConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.statisticsDuplexHandler = new StatisticsDuplexHandler(config);
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (config.isNetworkLogDebugEnabled()) {
            pipeline.addLast("logging-handler", new LoggingHandler(LogLevel.DEBUG));
        }


        pipeline.addLast("statistics-handler", statisticsDuplexHandler);
        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("connect-handler", new HeartbeatDuplexHandler(0, 60000));
        pipeline.addLast("processor-handler", getServiceDuplexHandler());
    }

    protected ServiceDuplexHandler getServiceDuplexHandler() {
        return InjectBeans.getBean(ServiceDuplexHandler.class);
    }
}
