package org.shallow.network;

import io.netty.channel.ChannelHandlerContext;
import org.shallow.handle.ProcessDuplexHandler;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessorAware;

public class ServiceDuplexHandler extends ProcessDuplexHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceDuplexHandler.class);

    public ServiceDuplexHandler(ProcessorAware processor) {
        super(processor);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("[channelInactive] - Clean inactive channel");
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("[channelInactive] - Clean exception caught channel");
        }
        ctx.close();
    }
}
