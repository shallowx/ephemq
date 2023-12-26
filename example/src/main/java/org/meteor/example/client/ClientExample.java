package org.meteor.example.client;

import io.netty.buffer.ByteBuf;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.ClientListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.TopicConfig;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicResponse;

public class ClientExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientExample.class);

    public static void main(String[] args) throws Exception {
        ClientExample example = new ClientExample();
        example.createTopic();
        example.delTopic();
    }

    private static final String EXAMPLE_TOPIC = "example-topic";
    private final Client client;

    public ClientExample() {
        this.client = new Client("default-client", new ClientConfig(), new DefaultClientListener());;
    }

    public void createTopic() throws Exception {
        // Supports multiple partitions, but only supports a single copy of a partition
        CreateTopicResponse response = client.createTopic(EXAMPLE_TOPIC, 10, 1);
        logger.info("topic:{} topic_id:{} partition:{}", response.getTopic(), response.getTopicId(), response.getPartitions());
        // do something
    }

    public void delTopic() throws Exception {
        DeleteTopicResponse response = client.deleteTopic(EXAMPLE_TOPIC);
        logger.info("response:{}", response.toString());
    }

    static class DefaultClientListener implements ClientListener {
        @Override
        public void onChannelActive(ClientChannel channel) {
            ClientListener.super.onChannelActive(channel);
        }

        @Override
        public void onChannelClosed(ClientChannel channel) {
            ClientListener.super.onChannelClosed(channel);
        }

        @Override
        public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
            ClientListener.super.onPushMessage(channel, signal, data);
        }

        @Override
        public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
            ClientListener.super.onTopicChanged(channel, signal);
        }

        @Override
        public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
            ClientListener.super.onNodeOffline(channel, signal);
        }

        @Override
        public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
            ClientListener.super.onSyncMessage(channel, signal, data);
        }
    }
}
