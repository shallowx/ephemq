package org.leopard.servlet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.leopard.ledger.Offset;

public interface PullDispatchProcessor {
    void register(int requestId, Channel channel);

    void dispatch(int requestId, String topic, String queue, short version, int ledgerId, int limit, Offset offset, ByteBuf payload);

    void shutdownGracefully();
}
