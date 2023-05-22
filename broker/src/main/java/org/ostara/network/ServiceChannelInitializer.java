package org.ostara.network;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.ostara.core.Beans;
import org.ostara.core.Config;
import org.ostara.management.Manager;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;

public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    private Config config;
    private Manager manager;
    private StatisticsDuplexHandler statisticsDuplexHandler;

    @Inject
    public ServiceChannelInitializer(Config config, Manager manager) {
        this.config = config;
        this.manager = manager;
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
        pipeline.addLast("connect-handler", new ConnectDuplexHandler(0, 60000));
        pipeline.addLast("processor-handler", Beans.getBean(ServiceDuplexHandler.class));
    }
}
