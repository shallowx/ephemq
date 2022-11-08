package org.shallow.nameserver.metadata;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.PartitionRecord;

import java.util.Set;

public class TopicManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final Manager manager;
    private final LoadingCache<String, Set<PartitionRecord>> cache;

    public TopicManager(Manager manager) {
        this.manager = manager;
        this.cache = Caffeine.newBuilder().build(key -> null);
    }

    public void add() {

    }

    public void remove() {

    }
}
