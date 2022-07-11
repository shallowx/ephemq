package org.shallow;

import io.netty.buffer.ByteBuf;

import java.nio.channels.Channel;

public interface ProcessorAware extends Aware{
    void process(Channel channel, int command, ByteBuf data);
}
