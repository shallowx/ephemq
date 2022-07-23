package org.shallow.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.shallow.invoke.InvokeAnswer;

public interface ProcessorAware extends Aware{

    default void onActive(ChannelHandlerContext ctx){}

    void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer);
}
