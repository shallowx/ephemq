package org.meteor.support;

import io.netty.channel.Channel;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * DefaultConnectionArraySet is an implementation of the Connection interface using a
 * thread-safe CopyOnWriteArraySet to manage the state of Channel objects.
 */
public class DefaultConnectionArraySet implements Connection {
    /**
     * A thread-safe set of channels that are ready for communication. This set is
     * managed using a CopyOnWriteArraySet to ensure that it can be safely accessed
     * and modified by multiple threads concurrently. Channels are added to this set
     * when they are active and ready, and removed when they are no longer in use.
     */
    private final Set<Channel> readyChannels = new CopyOnWriteArraySet<>();

    /**
     * Adds a channel to the connection manager for managing its state and readiness.
     * Only active channels will be added.
     *
     * @param channel the channel to be added to the connection manager
     */
    @Override
    public void add(Channel channel) {
        if (!channel.isActive()) {
            return;
        }
        readyChannels.add(channel);
    }

    /**
     * Removes the specified channel from the list of managed channels.
     *
     * @param channel the channel to be removed from the list of managed channels
     * @return true if the channel was successfully removed; false otherwise
     */
    @Override
    public boolean remove(Channel channel) {
        return readyChannels.remove(channel);
    }

    /**
     * Retrieves a set of channels that are ready for communication.
     *
     * @return a set of channels that are ready.
     */
    @Override
    public Set<Channel> getReadyChannels() {
        return readyChannels;
    }
}
