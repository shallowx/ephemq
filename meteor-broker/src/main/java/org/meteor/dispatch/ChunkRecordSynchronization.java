package org.meteor.dispatch;

import io.netty.channel.Channel;

import javax.annotation.concurrent.Immutable;

@Immutable
final class ChunkRecordSynchronization extends AbstractSynchronization<ChunkRecordHandler> {
    public ChunkRecordSynchronization(Channel channel, ChunkRecordHandler handler) {
        super(channel, handler);
    }
    @Override
    public String toString() {
        return "ChunkRecordSynchronization{" +
                ", channel=" + channel +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                '}';
    }
}
