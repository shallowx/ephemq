package org.meteor.remote.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;

/**
 * The DemoClientProcessor class implements the Processor interface to handle network
 * channel events and process received commands.
 */
public class DemoClientProcessor implements Processor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoClientProcessor.class);

    /**
     * Invoked when a channel becomes active.
     * This method handles actions that need to be performed when the channel transitions to an active state by
     * delegating to the default implementation in the Processor interface.
     *
     * @param channel  The network channel that has become active.
     * @param executor The executor used to handle subsequent tasks asynchronously.
     */
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Processor.super.onActive(channel, executor);
    }

    /**
     * Processes a command received on a network channel.
     *
     * @param channel  the {@link Channel} through which the command was received
     * @param command  an integer representing the specific command to be processed
     * @param data     the {@link ByteBuf} containing the data associated with the command
     * @param feedback the {@link InvokedFeedback} used for providing feedback after processing
     */
    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        // do nothing
        if (logger.isWarnEnabled()) {
            logger.warn("Readable bytes:{}", data.readableBytes());
        }
    }
}
