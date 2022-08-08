package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import java.util.concurrent.TimeUnit;

public class ClusterManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final LoadingCache<String, NodeRecord> nodeRecordCache;
    private final BrokerConfig config;

    public ClusterManager(BrokerConfig config) {
        this.config = config;

        this.nodeRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
            @Override
            public @Nullable NodeRecord load(String key) throws Exception {
                return null;
            }
        });
    }
}
