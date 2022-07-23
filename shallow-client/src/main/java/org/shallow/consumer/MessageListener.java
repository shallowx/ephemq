package org.shallow.consumer;

import io.netty.buffer.ByteBuf;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import static org.shallow.util.ByteUtil.buf2String;

@FunctionalInterface
public interface MessageListener {
    InternalLogger logger = InternalLoggerFactory.getLogger(MessageListener.class);

    MessageListener ACTIVE = (topic, queue, message) -> {
        if (logger.isDebugEnabled()) {
            logger.debug("topic=%s queue=%s message=%s", topic, queue, buf2String(message, message.readableBytes()));
        }
    };

    void onMessage(String topic, String queue, ByteBuf message);
}
