package org.meteor.remote.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.Processor;

public class DemoClientProcessor implements Processor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DemoClientProcessor.class);
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        Processor.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        // do nothing
        if (logger.isWarnEnabled()) {
            logger.warn("Readable bytes:{}", data.readableBytes());
        }
    }
}
