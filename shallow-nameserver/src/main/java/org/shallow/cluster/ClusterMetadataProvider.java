package org.shallow.cluster;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.api.MetaMappedFileAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class ClusterMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MetaMappedFileAPI api;

    public ClusterMetadataProvider(MetaMappedFileAPI api, EventExecutor cacheExecutor, EventExecutor apiExecutor, long expired) {
        this.api = api;
        this.cacheExecutor = cacheExecutor;
        this.apiExecutor = apiExecutor;
    }
}
