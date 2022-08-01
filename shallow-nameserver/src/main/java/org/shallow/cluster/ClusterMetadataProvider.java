package org.shallow.cluster;

import io.netty.util.concurrent.EventExecutor;
import org.shallow.api.MappedFileAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class ClusterMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MappedFileAPI api;

    public ClusterMetadataProvider(MappedFileAPI api, EventExecutor cacheExecutor, EventExecutor apiExecutor, long expired) {
        this.api = api;
        this.cacheExecutor = cacheExecutor;
        this.apiExecutor = apiExecutor;
    }
}
