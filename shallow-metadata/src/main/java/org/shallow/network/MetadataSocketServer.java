package org.shallow.network;

import org.shallow.internal.MetaConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class MetadataSocketServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataSocketServer.class);

    private final MetaConfig config;

    public MetadataSocketServer(MetaConfig config) {
        this.config = config;
    }

    public void start() {}

    public void shutdownGracefully() {}
}
