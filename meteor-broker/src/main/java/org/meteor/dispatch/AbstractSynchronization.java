package org.meteor.dispatch;

import io.netty.channel.Channel;
import org.meteor.common.message.Offset;

/**
 * The AbstractSynchronization class provides a base abstraction for managing
 * synchronization operations linked to a specific channel and handler.
 *
 * @param <T> the type of the handler.
 */
abstract class AbstractSynchronization<T> {
    /**
     * Represents the channel associated with the specific synchronization
     * operations being managed by the AbstractSynchronization instance.
     * This channel is used to coordinate and handle the communication
     * and synchronization tasks.
     */
    protected final Channel channel;
    /**
     * The handler that will be used for synchronization operations.
     * This is a protected final field, ensuring that it can only
     * be initialized once and accessed within the class hierarchy.
     */
    protected final T handler;
    /**
     * Represents the offset to dispatch from within a synchronization operation.
     * Utilized in tracking and managing the synchronization state based on an epoch and index.
     */
    protected Offset dispatchOffset;
    /**
     * Indicates whether the synchronization context is currently being followed.
     * This volatile boolean variable ensures that changes to its value are
     * visible across different threads, providing thread-safe synchronization.
     */
    protected volatile boolean followed = false;

    /**
     * Constructs an instance of AbstractSynchronization with the specified channel and handler.
     *
     * @param channel the channel associated with this synchronization
     * @param handler the handler responsible for processing this synchronization
     */
    public AbstractSynchronization(Channel channel, T handler) {
        this.channel = channel;
        this.handler = handler;
    }

    /**
     * Retrieves the channel associated with this synchronization.
     *
     * @return the channel associated with this synchronization instance.
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Retrieves the handler associated with this synchronization.
     *
     * @return the handler of type T.
     */
    public T getHandler() {
        return handler;
    }

    /**
     * Retrieves the current dispatch offset.
     *
     * @return the current dispatch offset.
     */
    public Offset getDispatchOffset() {
        return dispatchOffset;
    }

    /**
     * Sets the dispatch offset for the synchronization operation.
     *
     * @param dispatchOffset the offset to set for dispatching
     */
    public void setDispatchOffset(Offset dispatchOffset) {
        this.dispatchOffset = dispatchOffset;
    }

    /**
     * Checks if the synchronization process is currently being followed.
     *
     * @return true if the synchronization is being followed, false otherwise.
     */
    public boolean isFollowed() {
        return followed;
    }

    /**
     * Updates the followed status of the synchronization.
     *
     * @param followed the new followed status to set; true if the synchronization
     *                 is to be followed, false otherwise.
     */
    public void setFollowed(boolean followed) {
        this.followed = followed;
    }
}
