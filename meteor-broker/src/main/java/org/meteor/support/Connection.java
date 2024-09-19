package org.meteor.support;

import io.netty.channel.Channel;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This interface defines a contract for managing connections, allowing channels to be added,
 * removed, and retrieved based on their readiness state.
 */
public interface Connection {
    /**
     * Adds a channel to the connection manager for managing its state and readiness.
     *
     * @param channel the channel to be added to the connection manager
     */
    void add(Channel channel);

    /**
     * Removes the specified channel from the connection list.
     *
     * @param channel the channel to be removed from the connection
     * @return true if the channel was successfully removed; false otherwise
     */
    boolean remove(Channel channel);

    /**
     * Retrieves a set of channels that are ready for communication.
     *
     * @return a set of channels that are ready, or null if no channels are ready.
     */
    @Nullable
    Set<Channel> getReadyChannels();
}
