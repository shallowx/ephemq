package org.meteor.internal;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.client.core.ClientConfig;
import org.meteor.config.CommonConfig;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;

public class InternalClientChannelTest {
    /**
     * The Netty channel used for communication in tests.
     * <p>
     * This is an instance of the `Channel` interface, specifically
     * initialized as an `EmbeddedChannel` during the setup of the tests.
     * It is utilized by the `InternalClientChannelTest` class for
     * creating and testing an `InternalClientChannel` instance.
     */
    private Channel embeddedChannel;
    /**
     * Represents an instance of `InternalClientChannel` used in the `InternalClientChannelTest` class.
     * This variable is initialized in the `setUp` method and employed for testing various channel behaviors.
     *
     * @see InternalClientChannel
     */
    private InternalClientChannel clientChannel;

    /**
     * Sets up the test environment before each test method is run. This method initializes
     * the embeddedChannel and clientChannel for use in test cases.
     *
     * @throws Exception if any unexpected errors occur during the setup process
     */
    @Before
    public void setUp() throws Exception {
        embeddedChannel = new EmbeddedChannel();
        clientChannel = new InternalClientChannel(new ClientConfig(), embeddedChannel, new InetSocketAddress(9527),
                new CommonConfig(new Properties()));
    }

    /**
     * Tests the internal functionalities of the `InternalClientChannel`.
     * <p>
     * This test method performs the following verifications:
     * 1. Retrieves the `Channel` instance from the `clientChannel` instance and asserts it is not null.
     * 2. Gets the `SocketAddress` the `clientChannel` is bound to and asserts it is not null.
     * 3. Checks the `ByteBufAllocator` instance associated with the `clientChannel` and asserts it is not null.
     * <p>
     * Assertions:
     * - `Channel` instance should not be null.
     * - `SocketAddress` instance should not be null.
     * - `ByteBufAllocator` instance should not be null.
     */
    @Test
    public void testInternalChannel() {
        Channel channel = clientChannel.channel();
        Assertions.assertNotNull(channel);
        SocketAddress address = clientChannel.address();
        Assertions.assertNotNull(address);
        ByteBufAllocator allocator = clientChannel.allocator();
        Assertions.assertNotNull(allocator);
    }

    /**
     * Closes the `embeddedChannel` and `clientChannel` after each test is executed.
     * <p>
     * This method is annotated with `@After`, indicating that it runs after each test method in the class.
     * It ensures that resources are properly released and cleaned up to maintain a consistent test environment.
     *
     * @throws Exception if an error occurs during the closing of channels.
     */
    @After
    public void tearDown() throws Exception {
        embeddedChannel.close();
        clientChannel.close();
    }
}
