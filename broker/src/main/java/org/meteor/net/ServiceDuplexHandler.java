package org.meteor.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.meteor.coordinatio.Coordinator;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.handle.ProcessDuplexHandler;
import org.meteor.remote.processor.Processor;

public class ServiceDuplexHandler extends ProcessDuplexHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);

    private final Coordinator manager;

    public ServiceDuplexHandler(Coordinator manager, Processor processor) {
        super(processor);
        this.manager = manager;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        logger.debug("Service duplex inactive channel, and local_address={} remote_address={}", channel.localAddress().toString(), channel.remoteAddress().toString());
        manager.getConnectionManager().remove(channel);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        logger.debug("Service duplex caught channel, and local_address={} remote_address={}", channel.localAddress().toString(), channel.remoteAddress().toString());
        manager.getConnectionManager().remove(channel);
        ctx.close();
    }
}
