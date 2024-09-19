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
    /**
     *
     */
    private Channel embeddedChannel;
    /**
     * An instance of InternalClientChannel used for handling internal client communication.
     * It extends the ClientChannel class and is tailored for internal interactions within the system.
     * This instance includes additional configuration settings provided through the CommonConfig object.
     */
    private InternalClientChannel clientChannel;
    /**
     * An instance of {@code InternalClient} used for testing purposes within the {@code InternalClientTest} class.
     * This client is configured with specific settings and channels for embedded testing scenarios.
     */
    private InternalClient client;
    /**
     * The clientConfig variable holds the configuration settings for the internal client.
     * This configuration is provided by the ClientConfig class and includes various parameters
     * such as bootstrap addresses, socket preferences, buffer sizes, and timeout settings.
     * <p>
     * It is initialized during the setup phase and used to configure the internal client channel
     * and other components within the InternalClientTest class.
     */
    private ClientConfig clientConfig;

    /**
     * Sets up the test environment by initializing necessary components and configurations.
     *
     * @throws Exception if an error occurs during setup
     */
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

    /**
     * Tests the functionality of creating a client channel through the internal client.
     *
     * @throws Exception if an error occurs during the creation of the client channel.
     *
     * The test performs the following assertions on the created ClientChannel:
     * - The channel object itself is not null.
     * - The channel's address is not null.
     * - The channel's unique identifier (ID) is not null.
     * - The channel's byte buffer allocator is not null.
     * - The underlying Netty channel object is not null.
     * - The channel is actively connected.
     */
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

    /**
     * Tears down the test environment by closing the channels and the client.
     *
     * This method is annotated with {@link After}, which means it runs after each test case.
     * It ensures that resources such as the embedded channel, client channel,
     * and client are properly closed and cleaned up after each test.
     *
     * @throws Exception if an error occurs during the closing of channels or client.
     */
    @After
    public void tearDown() throws Exception {
        embeddedChannel.close();
        clientChannel.close();
        client.close();
    }
}
