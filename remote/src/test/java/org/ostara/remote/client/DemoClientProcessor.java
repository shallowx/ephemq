package org.ostara.remote.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.Processor;

public class DemoClientProcessor implements Processor {
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Processor.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        // do nothing
    }
}
