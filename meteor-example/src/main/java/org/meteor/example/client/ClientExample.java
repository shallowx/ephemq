package org.meteor.example.client;

import io.netty.buffer.ByteBuf;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicResponse;

public class ClientExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClientExample.class);
    private static final String EXAMPLE_TOPIC = "example-topic";
    private final Client client;


    public static void main(String[] args) throws Exception {
        ClientExample example = new ClientExample();
        example.createTopic();
        example.delTopic();
    }

    public ClientExample() {
        this.client = new Client("default-client", new ClientConfig(), new DefaultCombineListener());
    }

    public void createTopic() throws Exception {
        // Supports multiple partitions, but only supports a single copy of a partition
        CreateTopicResponse response = client.createTopic(EXAMPLE_TOPIC, 10, 1);
        if (logger.isInfoEnabled()) {
            logger.info("topic[{}] topic_id[{}] partition[{}]", response.getTopic(), response.getTopicId(), response.getPartitions());
        }
        // do something
    }

    public void delTopic() throws Exception {
        DeleteTopicResponse response = client.deleteTopic(EXAMPLE_TOPIC);
        if (logger.isInfoEnabled()) {
            logger.info("response[{}]", response.toString());
        }
    }

    static class DefaultCombineListener implements CombineListener {
        @Override
        public void onChannelActive(ClientChannel channel) {
            CombineListener.super.onChannelActive(channel);
        }

        @Override
        public void onChannelClosed(ClientChannel channel) {
            CombineListener.super.onChannelClosed(channel);
        }

        @Override
        public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
            CombineListener.super.onPushMessage(channel, signal, data);
        }

        @Override
        public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
            CombineListener.super.onTopicChanged(channel, signal);
        }

        @Override
        public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
            CombineListener.super.onNodeOffline(channel, signal);
        }

        @Override
        public void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
            CombineListener.super.onSyncMessage(channel, signal, data);
        }
    }
}
