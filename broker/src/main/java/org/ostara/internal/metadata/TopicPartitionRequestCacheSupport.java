package org.ostara.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Partition;
import org.ostara.common.util.StringUtils;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.config.ServerConfig;
import org.ostara.ledger.LedgerEngine;
import org.ostara.network.MessageProcessorAware;

/**
 * Thread safety is guaranteed by aware {@link MessageProcessorAware} variable {@code commandExecutor} of the thread.
 */
public class TopicPartitionRequestCacheSupport {

    private static final InternalLogger logger =
            InternalLoggerFactory.getLogger(TopicPartitionRequestCacheSupport.class);

    private final PartitionLeaderAssignorFactory factory;
    private final LoadingCache<String, Set<Partition>> cache;
    private final ClusterNodeCacheSupport nodeCacheWriterSupport;
    private final LedgerEngine engine;
    private final ServerConfig config;

    public TopicPartitionRequestCacheSupport(ServerConfig config, ResourceContext context) {
        this.config = config;
        this.factory = new PartitionLeaderAssignorFactory(context, config);
        this.nodeCacheWriterSupport = context.getNodeCacheSupport();
        this.engine = context.getLedgerEngine();

        this.cache = Caffeine.newBuilder().refreshAfterWrite(config.getMetadataRefreshMs(), TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<Partition> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    public void createTopic(String topic, int partitionLimit, int replicateLimit, Promise<Void> promise) {
        if (partitionLimit <= 0) {
            promise.tryFailure(new IllegalArgumentException("Number of partition limit must be larger than 0"));
            return;
        }

        if (replicateLimit <= 0) {
            promise.tryFailure(new IllegalArgumentException("Number of replicate limit must be larger than 0"));
            return;
        }

        if (replicateLimit > partitionLimit) {
            promise.tryFailure(new IllegalArgumentException(
                    "The cluster does not have enough nodes to allocate replicates, and node_count="
                            + nodeCacheWriterSupport.size()));
            return;
        }

        if (StringUtils.isNullOrEmpty(topic)) {
            promise.tryFailure(new IllegalArgumentException("Topic cannot be empty"));
            return;
        }

        Set<Partition> partitions = cache.get(topic);
        if (partitions != null && !partitions.isEmpty()) {
            promise.tryFailure(new IllegalArgumentException("Topic already exists"));
            return;
        }

        try {
            partitions = factory.assign(topic, partitionLimit, replicateLimit);

            cache.put(topic, partitions);
            for (Partition partition : partitions) {
                engine.initLog(topic, partition.getId(), partition.getEpoch(), partition.getLedgerId());
            }
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
            logger.error("Failed to create topic, topic={}", topic, e);
        }
    }

    public void delTopic(String topic, Promise<Void> promise) {
        if (StringUtils.isNullOrEmpty(topic)) {
            promise.tryFailure(new IllegalArgumentException("Topic cannot be empty"));
        }

        cache.invalidate(topic);
        promise.trySuccess(null);
    }

    public Map<String, Set<Partition>> loadAll(List<String> topics) throws Exception {
        Map<String, Set<Partition>> partitions = new HashMap<>();
        if (topics == null || topics.isEmpty()) {
            for (Map.Entry<String, Set<Partition>> entry : cache.asMap().entrySet()) {
                String topic = entry.getKey();
                Set<Partition> values = entry.getValue();

                if (values == null || values.isEmpty()) {
                    continue;
                }
                partitions.put(topic, values);
            }
        } else {
            for (String topic : topics) {
                Set<Partition> values = this.cache.get(topic);
                if (values == null || values.isEmpty()) {
                    continue;
                }
                partitions.put(topic, values);
            }
        }
        return partitions;
    }
}
