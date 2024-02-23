package org.meteor.remote.invoke;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;

public interface Processor {
    void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback);

    default void onActive(Channel channel, EventExecutor executor) {
    }
}
