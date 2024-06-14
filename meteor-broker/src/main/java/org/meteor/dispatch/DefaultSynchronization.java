package org.meteor.dispatch;

import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.ints.IntSet;
import javax.annotation.concurrent.Immutable;

@Immutable
final class DefaultSynchronization extends AbstractSynchronization<DefaultHandler> {
    private final IntSet markers;

    public DefaultSynchronization(Channel channel, DefaultHandler handler, IntSet markers) {
        super(channel, handler);
        this.markers = markers;
    }

    public IntSet getMarkers() {
        return markers;
    }

    @Override
    public String toString() {
        return "RecordSynchronization{" +
                "markers=" + markers +
                ", channel=" + channel +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                '}';
    }
}
