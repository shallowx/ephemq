package org.shallow.remote;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.processor.ProcessorAware;

public class RemoteServerProcessorAwareTest implements ProcessorAware {

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        ProcessorAware.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> rejoin) {
        switch (command) {
            case 1 -> {
                System.out.println();
            }
            case 2 -> {
                System.out.println("a");
            }
        }
    }

    private void echo(Channel channel, ByteBuf buf, InvokeAnswer<ByteBuf> rejoin) {
        ByteBuf echo = buf.retainedSlice();

        int length = buf.readInt();
    }

}
