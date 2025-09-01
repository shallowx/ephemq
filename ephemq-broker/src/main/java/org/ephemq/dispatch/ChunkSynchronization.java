package org.ephemq.dispatch;

import io.netty.channel.Channel;
import javax.annotation.concurrent.Immutable;

/**
 * This class represents a final implementation of the {@link AbstractSynchronization} framework,
 * specifically dealing with synchronization operations for chunks.
 * It is linked to a specific {@link Channel} and a {@link ChunkHandler}.
 * <p>
 * The class is immutable, meaning that its state cannot be modified after its construction.
 * It inherits the functionalities of managing synchronization operations from its parent class.
 */
@Immutable
final class ChunkSynchronization extends AbstractSynchronization<ChunkHandler> {
    /**
     * Constructs a new ChunkSynchronization instance with the specified channel and handler.
     *
     * @param channel The communication channel associated with this synchronization.
     * @param handler The handler responsible for processing this synchronization.
     */
    public ChunkSynchronization(Channel channel, ChunkHandler handler) {
        super(channel, handler);
    }

    /**
     * Returns a string representation of the ChunkSynchronization object.
     *
     * @return a string that includes the channel, handler, dispatchOffset, and followed status of this ChunkSynchronization.
     */
    @Override
    public String toString() {
        return "ChunkSynchronization (channel=%s, handler=%s, dispatchOffset=%s, followed=%s)".formatted(channel, handler, dispatchOffset, followed);
    }
}
