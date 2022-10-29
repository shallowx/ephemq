package org.shallow.nameserver;

import io.netty.channel.ChannelHandlerContext;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.remote.handle.ProcessDuplexHandler;
import org.shallow.remote.processor.ProcessorAware;

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
