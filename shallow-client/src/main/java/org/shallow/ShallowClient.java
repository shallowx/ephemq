package org.shallow;

import org.shallow.handle.ProcessDuplexHandler;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.net.SocketAddress;
import java.util.List;

public class ShallowClient {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProcessDuplexHandler.class);

    private String name;
    private ClientConfig config;
    private List<SocketAddress> bootstrapAddress;
}
