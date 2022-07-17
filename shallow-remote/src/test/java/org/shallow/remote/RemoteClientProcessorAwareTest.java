package org.shallow.remote;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.shallow.invoke.InvokeRejoin;
import org.shallow.processor.ProcessorAware;

public class RemoteClientProcessorAwareTest implements ProcessorAware {

    @Override
    public void onActive(ChannelHandlerContext ctx) {
        ProcessorAware.super.onActive(ctx);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeRejoin<ByteBuf> rejoin) {

    }
}
