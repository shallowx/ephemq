package org.ostara.network;

import com.google.inject.Inject;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.management.Manager;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.processor.ProcessorAware;

public class ServiceDuplexHandler extends ProcessDuplexHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);

    private final Manager manager;

    @Inject
    public ServiceDuplexHandler(Manager manager, ProcessorAware processor) {
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
