package org.ostara.remote;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessorAware;

public class DemoClientProcessor implements ProcessorAware {
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        ProcessorAware.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }
}
