package org.meteor.internal;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.client.core.ClientConfig;
import org.meteor.config.CommonConfig;

public class InternalClientChannelTest {
    private Channel embeddedChannel;
    private InternalClientChannel clientChannel;

    @Before
    public void setUp() throws Exception {
        embeddedChannel = new EmbeddedChannel();
        clientChannel = new InternalClientChannel(new ClientConfig(), embeddedChannel, new InetSocketAddress(9527),
                new CommonConfig(new Properties()));
    }

    @Test
    public void testInternalChannel() {
        Channel channel = clientChannel.channel();
        Assertions.assertNotNull(channel);
        SocketAddress address = clientChannel.address();
        Assertions.assertNotNull(address);
        ByteBufAllocator allocator = clientChannel.allocator();
        Assertions.assertNotNull(allocator);
    }

    @After
    public void tearDown() throws Exception {
        embeddedChannel.close();
        clientChannel.close();
    }
}
