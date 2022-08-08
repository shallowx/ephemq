package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;

import java.util.concurrent.TimeUnit;

public class TopicManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final BrokerConfig config;
    private final LoadingCache<String, PartitionRecord> topicRecordCache;

    public TopicManager(BrokerConfig config) {
        this.config = config;

        this.topicRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
            @Override
            public @Nullable PartitionRecord load(String key) throws Exception {
                return null;
            }
        });
    }
}
