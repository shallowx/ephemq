package org.meteor.remoting;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.handle.ProcessDuplexHandler;
import org.meteor.remote.invoke.Processor;
import org.meteor.support.Manager;

public class ServiceDuplexHandler extends ProcessDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);
    private final Manager manager;

    public ServiceDuplexHandler(Manager manager, Processor processor) {
        super(processor);
        this.manager = manager;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        manager.getConnection().remove(channel);
        super.channelInactive(ctx);
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex inactive channel, and local_address[{}], remote_address[{}]",
                    channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel channel = ctx.channel();
        manager.getConnection().remove(channel);
        ctx.close();
        if (logger.isDebugEnabled()) {
            logger.debug("Service duplex caught channel, and local address[{}], remote address[{}]",
                    channel.localAddress().toString(), channel.remoteAddress().toString());
        }
    }
}
