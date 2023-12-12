package org.meteor.remote.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.remote.invoke.InvokeAnswer;

public interface Processor {

    default void onActive(Channel channel, EventExecutor executor) {
    }

    void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer);
}
