package org.meteor.dispatch;

import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.Immutable;

@Immutable
public class RecordSynchronization extends Synchronization<RecordHandler> {
    private final IntSet markers;

    public RecordSynchronization(Channel channel, RecordHandler handler, IntSet markers) {
        super(channel, handler);
        this.markers = markers;
    }

    public IntSet getMarkers() {
        return markers;
    }

    @Override
    public String toString() {
        return "Synchronization{" +
                "markers=" + markers +
                ", channel=" + channel +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                '}';
    }
}
