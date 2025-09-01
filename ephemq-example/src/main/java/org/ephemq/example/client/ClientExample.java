package org.ephemq.example.client;

import io.netty.buffer.ByteBuf;
import org.ephemq.client.core.Client;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.client.core.ClientConfig;
import org.ephemq.client.core.CombineListener;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.proto.client.MessagePushSignal;
import org.ephemq.remote.proto.client.NodeOfflineSignal;
import org.ephemq.remote.proto.client.SyncMessageSignal;
import org.ephemq.remote.proto.client.TopicChangedSignal;
import org.ephemq.remote.proto.server.CreateTopicResponse;
import org.ephemq.remote.proto.server.DeleteTopicResponse;

/**
 * ClientExample demonstrates basic operations with a client such as creating and deleting topics.
 * It uses a default client configuration and a default combine listener for handling various client events.
 */
public class ClientExample {
    /**
     * Logger instance for the ClientExample class, used for logging client operations and events.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientExample.class);
    /**
     * The topic name used for demonstration purposes in the ClientExample class.
     * This topic is created and deleted by the ClientExample class methods to show
     * basic client operations.
     */
    private static final String EXAMPLE_TOPIC = "example-topic";
    /**
     * The client instance used for performing operations such as creating and deleting topics.
     * It is configured with default settings and a default combine listener to handle client events.
     */
    private final Client client;

    /**
     * The main method serves as the entry point for the application.
     * It demonstrates the creation and deletion of a topic using the ClientExample class.
     *
     * @param args Command-line arguments passed to the application.
     * @throws Exception if any error occurs during topic creation or deletion.
     */
    public static void main(String[] args) throws Exception {
        ClientExample example = new ClientExample();
        example.createTopic();
        example.delTopic();
    }

    /**
     * Constructs a new ClientExample instance with a default client configuration and listener.
     * The client is initialized with the name "default-client", an instance of ClientConfig, and a DefaultCombineListener.
     */
    public ClientExample() {
        this.client = new Client("default-client", new ClientConfig(), new DefaultCombineListener());
    }

    /**
     * Creates a topic with a specified number of partitions and replication factor.
     * <p>
     * Creates a topic named "example-topic" with 10 partitions and a replication factor of 1.
     * Logs information about the created topic including its name, ID, and the number of partitions.
     *
     * @throws Exception if the topic creation fails due to client errors or other unforeseen issues.
     */
    public void createTopic() throws Exception {
        // Supports multiple partitions, but only supports a single copy of a partition
        CreateTopicResponse response = client.createTopic(EXAMPLE_TOPIC, 10, 1);
        if (logger.isInfoEnabled()) {
            logger.info("topic[{}] topic_id[{}] partition[{}]", response.getTopic(), response.getTopicId(), response.getPartitions());
        }
        // do something
    }

    /**
     * Deletes a specific topic identified by EXAMPLE_TOPIC using the client instance.
     *
     * @throws Exception if an error occurs during the deletion process
     */
    public void delTopic() throws Exception {
        DeleteTopicResponse response = client.deleteTopic(EXAMPLE_TOPIC);
        if (logger.isInfoEnabled()) {
            logger.info("response[{}]", response.toString());
        }
    }

    static class DefaultCombineListener implements CombineListener {
        /**
         * Invoked when a channel becomes active.
         *
         * @param channel the channel that became active
         */
        @Override
        public void onChannelActive(ClientChannel channel) {
            CombineListener.super.onChannelActive(channel);
        }

        /**
         * Invoked when the channel is closed.
         *
         * @param channel the {@link ClientChannel} that has been closed
         */
        @Override
        public void onChannelClosed(ClientChannel channel) {
            CombineListener.super.onChannelClosed(channel);
        }

        /**
         * Handles the push message event received from the client channel.
         *
         * @param channel the client channel from which the message is received
         * @param signal the signal representing the push message
         * @param data the buffer containing the data of the push message
         */
        @Override
        public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
            CombineListener.super.onPushMessage(channel, signal, data);
        }

        /**
         * Handles the event when a topic change occurs.
         *
         * @param channel the client channel through which the topic change signal is received
         * @param signal the signal that indicates a topic change has occurred
         */
        @Override
        public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
            CombineListener.super.onTopicChanged(channel, signal);
        }

        /**
         * Invoked when a node goes offline.
         *
         * @param channel the client channel associated with the offline node
         * @param signal the signal indicating the node is offline
         */
        @Override
        public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
            CombineListener.super.onNodeOffline(channel, signal);
        }

        /**
         * Handles synchronized messages received by the client channel.
         *
         * @param channel the client channel that received the synchronized message
         * @param signal the signal indicating the type of synchronized message
         * @param data the data payload of the synchronized message
         */
        @Override
        public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
            CombineListener.super.onSyncMessage(channel, signal, data);
        }
    }
}
