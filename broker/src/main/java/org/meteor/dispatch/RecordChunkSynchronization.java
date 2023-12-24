package org.meteor.dispatch;

import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.Immutable;

@Immutable
public class RecordChunkSynchronization extends Synchronization<RecordChunkHandler> {
    public RecordChunkSynchronization(Channel channel, RecordChunkHandler handler) {
        super(channel, handler);
    }

    @Override
    public String toString() {
        return "Synchronization{" +
                ", channel=" + channel +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                '}';
    }
}
