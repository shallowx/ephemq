package org.shallow.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.client.Client;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.NodeRecord;
import org.shallow.internal.config.BrokerConfig;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusterNodeCache {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterNodeCache.class);

    private final BrokerConfig config;
    private final Client internalClient;
    private final LoadingCache<String, Set<NodeRecord>> nodes;

    public ClusterNodeCache(BrokerConfig config, Client internalClient) {
        this.config = config;
        this.internalClient = internalClient;


        this.nodes = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        try {
                            return this.load(key);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                });
    }

    public void start() throws Exception {

    }

    public void register() throws Exception {

    }

    public void unregister() throws Exception {

    }

    public Set<NodeRecord> load(String cluster) {
        return null;
    }

    private void heartbeat() {

    }
}
