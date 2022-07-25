package org.shallow.remote;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.shallow.codec.MessageDecoder;
import org.shallow.codec.MessageEncoder;
import org.shallow.handle.ConnectDuplexHandler;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class MetaServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetaServerChannelInitializer.class);


    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();


        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("connect-handler", new ConnectDuplexHandler(0, 60000));
        pipeline.addLast("service-handler", new MetaServiceDuplexHandler(new MetaProcessorAware()));
    }
}
