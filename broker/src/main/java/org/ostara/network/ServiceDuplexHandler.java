package org.ostara.network;

import io.netty.channel.ChannelHandlerContext;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.processor.ProcessorAware;

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
