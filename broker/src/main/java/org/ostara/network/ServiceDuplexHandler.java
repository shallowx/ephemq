package org.ostara.network;

import com.google.inject.Inject;
import io.netty.channel.ChannelHandlerContext;
import org.ostara.management.Manager;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.processor.ProcessorAware;

public class ServiceDuplexHandler extends ProcessDuplexHandler {

    private Manager manager;

    @Inject
    public ServiceDuplexHandler(Manager manager, ProcessorAware processor) {
        super(processor);
        this.manager = manager;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        manager.getConnectionManager().remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        manager.getConnectionManager().remove(ctx.channel());
        ctx.close();
    }
}
