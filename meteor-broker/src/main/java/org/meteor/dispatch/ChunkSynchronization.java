package org.meteor.dispatch;

import io.netty.channel.Channel;
import javax.annotation.concurrent.Immutable;

@Immutable
final class ChunkSynchronization extends AbstractSynchronization<ChunkHandler> {
    public ChunkSynchronization(Channel channel, ChunkHandler handler) {
        super(channel, handler);
    }

    @Override
    public String toString() {
        return "(" +
                ", channel=" + channel +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                ')';
    }
}
