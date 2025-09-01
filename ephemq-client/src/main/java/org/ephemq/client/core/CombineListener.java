package org.ephemq.client.core;

import io.netty.buffer.ByteBuf;
import org.ephemq.remote.proto.client.MessagePushSignal;
import org.ephemq.remote.proto.client.NodeOfflineSignal;
import org.ephemq.remote.proto.client.SyncMessageSignal;
import org.ephemq.remote.proto.client.TopicChangedSignal;

/**
 * The CombineListener interface defines a set of methods for handling various events
 * that occur within a client channel, such as activation, closure, message pushes,
 * topic changes, node offline events, and synchronization messages. Implementors
 * of this interface can optionally override these methods to perform custom
 * processing for each event type.
 */
public interface CombineListener {
    /**
     * Invoked when a client channel becomes active.
     *
     * @param channel the client channel that has become active
     */
    default void onChannelActive(ClientChannel channel) {
    }

    /**
     * Invoked when a client channel is closed. This method is called to handle any
     * necessary cleanup or finalization tasks when the channel is no longer active.
     *
     * @param channel the client channel that has been closed
     */
    default void onChannelClosed(ClientChannel channel) {
    }

    /**
     * Handles an incoming push message for the client channel.
     *
     * @param channel the client channel that received the push message
     * @param signal  the type of push message signal received
     * @param data    the data payload of the push message
     */
    default void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
    }

    /**
     * Handles the event when a topic within the client channel changes.
     *
     * @param channel the client channel where the topic change occurred
     * @param signal the signal containing details about the topic change event
     */
    default void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
    }

    /**
     * Triggered when a node goes offline within the client channel.
     *
     * @param channel the ClientChannel in which the node went offline
     * @param signal  the NodeOfflineSignal containing details about the offline event
     */
    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
    }

    /**
     * Handles synchronization messages received from a client channel.
     *
     * @param channel the client channel from which the message was received
     * @param signal the synchronization message signal indicating the type or purpose of the message
     * @param data the content of the synchronization message as a ByteBuf
     */
    default void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
    }

    /**
     * Method to be called when the listener has completed its tasks.
     *
     * <ul>
     * <li>This method serves as a signal that the listener has finished its execution.
     * <li>It can be overridden by implementors to provide custom completion logic.
     * <li>If the listener's execution is interrupted, this method will throw an {@link InterruptedException}.
     * </ul>
     *
     * @throws InterruptedException if the thread executing this method is interrupted
     */
    default void listenerCompleted() throws InterruptedException {
    }
}
