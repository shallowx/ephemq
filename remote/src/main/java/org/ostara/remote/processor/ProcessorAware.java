package org.ostara.remote.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.remote.invoke.InvokeAnswer;

public interface ProcessorAware extends Aware{

    default void onActive(Channel channel, EventExecutor executor){}

    void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer);
}
