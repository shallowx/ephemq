package org.shallow.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.client.Client;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.PartitionRecord;
import org.shallow.internal.config.BrokerConfig;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TopicPartitionRequestCache {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicPartitionRequestCache.class);

    private final BrokerConfig config;
    private final Client internalClient;
    private final PartitionLeaderElector leaderElector;

    private final LoadingCache<String, Set<PartitionRecord>> topics;

    public TopicPartitionRequestCache(BrokerConfig config, Client internalClient) {
        this.config = config;
        this.internalClient = internalClient;
        this.leaderElector = new PartitionLeaderElector();

        this.topics = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionRecord> load(String key) throws Exception {
                        try {
                            return this.load(key);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                });
    }

    public void createTopic(String topic, int partitions, int latencies, Promise<Void> promise) {
        try {
            Set<PartitionRecord> partitionRecords = load(topic);
            if (partitionRecords != null && !partitionRecords.isEmpty()) {
                promise.tryFailure(new IllegalStateException(String.format("Topic already exists, and topic=%s", topic)));
                return;
            }


        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void delTopic(String topic, Promise<Void> promise) {
        try {
            Set<PartitionRecord> partitionRecords = load(topic);
            if (partitionRecords == null || partitionRecords.isEmpty()) {
                promise.tryFailure(new IllegalStateException(String.format("Topic not exists, and topic=%s", topic)));
                return;
            }


        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public Set<PartitionRecord> load(String topic) throws Exception {
        return null;
    }
}
