package org.shallow.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.shallow.internal.MetadataConfig;
import org.shallow.internal.MetadataManager;
import org.shallow.codec.MessageDecoder;
import org.shallow.codec.MessageEncoder;
import org.shallow.handle.ConnectDuplexHandler;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class MetadataServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataServerChannelInitializer.class);

    private final MetadataManager metaManager;

    public MetadataServerChannelInitializer(MetadataConfig config, MetadataManager metaManager) {
        this.metaManager = metaManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("encoder", MessageEncoder.instance());
        pipeline.addLast("decoder", new MessageDecoder());
        pipeline.addLast("connect-handler", new ConnectDuplexHandler(0, 60000));
        pipeline.addLast("service-handler", new MetadataServiceDuplexHandler(new MetadataProcessorAware(metaManager)));
    }
}
