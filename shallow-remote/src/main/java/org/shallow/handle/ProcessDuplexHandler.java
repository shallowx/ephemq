package org.shallow.handle;

import io.netty.channel.ChannelDuplexHandler;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class ProcessDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);
}
