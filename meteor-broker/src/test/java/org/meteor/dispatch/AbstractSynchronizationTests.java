package org.meteor.dispatch;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.common.message.Offset;

import javax.annotation.Nonnull;

public class AbstractSynchronizationTests {

    private ChannelId channelId;

    @Before
    public void before() {
        channelId = new ChannelId() {
            @Override
            public String asShortText() {
                return "1";
            }

            @Override
            public String asLongText() {
                return "1";
            }

            @Override
            public int compareTo(@Nonnull ChannelId o) {
                return 0;
            }
        };
    }

    @Test
    public void testGetChannel() {
        Channel channel = new EmbeddedChannel(channelId, false);
        TestAbstractSynchronization abstractSynchronization = new TestAbstractSynchronization(channel, "test");
        Assertions.assertEquals("1", abstractSynchronization.getChannel().id().asLongText());
        Assertions.assertEquals(channelId.asLongText(), abstractSynchronization.getChannel().id().asLongText());
        channel.close();
    }

    @Test
    public void testOthers() {
        Channel channel = new EmbeddedChannel(channelId, true);
        TestAbstractSynchronization abstractSynchronization = new TestAbstractSynchronization(channel, "test");
        Assertions.assertEquals("1", abstractSynchronization.getChannel().id().asLongText());
        Assertions.assertEquals(channelId.asLongText(), abstractSynchronization.getChannel().id().asLongText());
        Assertions.assertEquals(channelId, abstractSynchronization.getChannel().id());
        Assertions.assertNull(abstractSynchronization.dispatchOffset);

        Offset dispatchOffset = new Offset(0, 1000);
        abstractSynchronization.setDispatchOffset(dispatchOffset);
        Assertions.assertNotNull(abstractSynchronization.dispatchOffset);
        Assertions.assertEquals(dispatchOffset, abstractSynchronization.dispatchOffset);

        Assertions.assertFalse(abstractSynchronization.followed);
        abstractSynchronization.setFollowed(true);
        Assertions.assertTrue(abstractSynchronization.followed);
        channel.close();
    }

    static class TestAbstractSynchronization extends AbstractSynchronization<String> {

        /**
         * Constructs an instance of AbstractSynchronization with the specified channel and handler.
         *
         * @param channel the channel associated with this synchronization
         * @param handler the handler responsible for processing this synchronization
         */
        public TestAbstractSynchronization(Channel channel, String handler) {
            super(channel, handler);
        }
    }
}
