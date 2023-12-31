package org.meteor.remoting;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.meteor.coordinatior.Coordinator;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.handle.ProcessDuplexHandler;
import org.meteor.remote.processor.Processor;

public class ServiceDuplexHandler extends ProcessDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);
    private final Coordinator coordinator;

    public ServiceDuplexHandler(Coordinator coordinator, Processor processor) {
        super(processor);
        this.coordinator = coordinator;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        coordinator.getConnectionCoordinator().remove(channel);
        super.channelInactive(ctx);
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex inactive channel, and local_address[{}] remote_address[{}]", channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        coordinator.getConnectionCoordinator().remove(channel);
        ctx.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex caught channel, and local address[{}] and remote address[{}]", channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }
}
