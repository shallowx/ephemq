package org.leopard.ledger;

import io.netty.buffer.ByteBuf;

public interface LedgerTrigger {
    void onAppend(int limit, Offset tail);

    void onPull(int requestId, String queue, short version, int ledgerId, int limit, Offset head, ByteBuf buf);
}
