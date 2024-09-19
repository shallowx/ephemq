package org.meteor.dispatch;

import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.ints.IntSet;
import javax.annotation.concurrent.Immutable;

/**
 * The DefaultSynchronization class is an immutable final class that extends
 * the AbstractSynchronization class with a specific handler type of DefaultHandler.
 * It represents synchronization operations along with associated markers.
 */
@Immutable
final class DefaultSynchronization extends AbstractSynchronization<DefaultHandler> {
    /**
     * Stores a set of integer markers associated with this synchronization.
     * The markers are used to tag and identify specific synchronization operations.
     */
    private final IntSet markers;

    /**
     * Constructs a new DefaultSynchronization object with the specified channel, handler, and markers.
     *
     * @param channel the channel associated with this synchronization
     * @param handler the handler responsible for processing this synchronization
     * @param markers the set of integer markers associated with this synchronization
     */
    public DefaultSynchronization(Channel channel, DefaultHandler handler, IntSet markers) {
        super(channel, handler);
        this.markers = markers;
    }

    /**
     * Returns the set of markers associated with this synchronization object.
     *
     * @return an IntSet containing the markers
     */
    public IntSet getMarkers() {
        return markers;
    }

    /**
     * Returns a string representation of the DefaultSynchronization object.
     *
     * @return a string that represents the state of the DefaultSynchronization object,
     * including markers, channel, handler, dispatchOffset, and followed status.
     */
    @Override
    public String toString() {
        return STR."(markers=\{markers}, channel=\{channel}, handler=\{handler}, dispatchOffset=\{dispatchOffset}, followed=\{followed})";
    }
}
