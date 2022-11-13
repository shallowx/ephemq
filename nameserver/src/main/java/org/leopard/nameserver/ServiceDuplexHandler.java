package org.leopard.nameserver;

import io.netty.channel.ChannelHandlerContext;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.remote.handle.ProcessDuplexHandler;
import org.leopard.remote.processor.ProcessorAware;

public class ServiceDuplexHandler extends ProcessDuplexHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);

    public ServiceDuplexHandler(ProcessorAware processor) {
        super(processor);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Clean inactive channel<{}>", ctx.channel().toString());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Clean exception caught channel <{}>, error:{}", ctx.channel().toString(), cause);
        }

        ctx.close();
    }
}
