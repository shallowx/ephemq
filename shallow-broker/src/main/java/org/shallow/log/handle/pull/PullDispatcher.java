package org.shallow.log.handle.pull;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.log.Offset;

public interface PullDispatcher {
    void register(int requestId, Channel channel);

    void dispatch(int requestId, String topic, String queue, short version, int ledgerId, int limit, Offset offset, ByteBuf payload);

    void shutdownGracefully();
}
