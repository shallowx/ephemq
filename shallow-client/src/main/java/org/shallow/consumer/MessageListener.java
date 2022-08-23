package org.shallow.consumer;

import io.netty.buffer.ByteBuf;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.util.ByteUtil;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, String queue, ByteBuf message);
}
