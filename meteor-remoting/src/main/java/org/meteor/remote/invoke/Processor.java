package org.meteor.remote.invoke;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;

/**
 * The Processor interface defines a structure for processing commands received
 * on a network channel and handling channel activation events.
 */
public interface Processor {
    /**
     * Processes a command received on a network channel.
     *
     * @param channel  the {@link Channel} through which the command was received
     * @param command  an integer representing the specific command to be processed
     * @param data     the {@link ByteBuf} containing the data associated with the command
     * @param feedback the {@link InvokedFeedback} used for providing feedback after processing
     */
    void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback);

    /**
     * Invoked when a channel becomes active.
     * This method handles actions that need to be performed when the channel transitions to an active state.
     *
     * @param channel  The network channel that has become active.
     * @param executor The executor used to handle subsequent tasks asynchronously.
     */
    default void onActive(Channel channel, EventExecutor executor) {
    }
}
