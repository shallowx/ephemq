package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public interface LedgerTrigger {
    void onAppend(int ledgerId, int limit, Offset tail);

    void onPull(int requestId, String queue, int ledgerId, int limit, Offset head, ByteBuf buf);
}
