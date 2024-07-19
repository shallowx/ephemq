package org.meteor.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;
import org.meteor.config.CommonConfig;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;

public class InternalClientTest {
    private Channel embeddedChannel;
    private InternalClientChannel clientChannel;
    private InternalClient client;
    private ClientConfig clientConfig;

    @Before
    public void setUp() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ObjectArrayList<>() {{
            add("localhost:9527");
        }});
        embeddedChannel = new EmbeddedChannel();
        clientChannel = new InternalClientChannel(clientConfig, embeddedChannel, new InetSocketAddress(9527),
                new CommonConfig(new Properties()));

        client = new InternalClient("test-internal-client", clientConfig, new CombineListener() {
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

            @Override
            public void listenerCompleted() throws InterruptedException {
                CombineListener.super.listenerCompleted();
            }
        }, new CommonConfig(new Properties()));

        client.start();
    }

    @Test
    public void testInternalClient() throws Exception {
        ClientChannel ch = client.createClientChannel(clientConfig, embeddedChannel, new InetSocketAddress(9527));
        Assertions.assertNotNull(ch);
        Assertions.assertNotNull(ch.address());
        Assertions.assertNotNull(ch.id());
        Assertions.assertNotNull(ch.allocator());
        Assertions.assertNotNull(ch.channel());
        Assertions.assertTrue(ch.isActive());
    }

    @After
    public void tearDown() throws Exception {
        embeddedChannel.close();
        clientChannel.close();
        client.close();
    }
}
