package org.leopard.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Partition;
import org.leopard.common.util.StringUtils;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;
import org.leopard.ledger.LedgerEngine;
import org.leopard.network.MessageProcessorAware;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Thread safety is guaranteed by aware {@link MessageProcessorAware} variable {@code commandExecutor} of the thread.
 */
public class TopicPartitionRequestCacheWriterSupport {
    
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicPartitionRequestCacheWriterSupport.class);

    private final PartitionLeaderAssignor leaderAssignor;
    private final LoadingCache<String, Set<Partition>> cache;
    private final ClusterNodeCacheWriterSupport nodeCacheWriterSupport;
    private final LedgerEngine engine;

    public TopicPartitionRequestCacheWriterSupport(ServerConfig config, ResourceContext context) {
        this.leaderAssignor = new PartitionLeaderAssignor(context, config);
        this.nodeCacheWriterSupport = context.getNodeCacheWriterSupport();
        this.engine = context.getLedgerEngine();

        this.cache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
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
            promise.tryFailure(new IllegalArgumentException("The cluster does not have enough nodes to allocate replicates, and node_count=" + nodeCacheWriterSupport.size()));
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
            partitions = leaderAssignor.assign(topic, partitionLimit, replicateLimit);
            this.cache.put(topic, partitions);

            for (Partition partition : partitions) {
                engine.initLog(topic, partition.getId(), partition.getEpoch(), partition.getLedgerId());
            }
        } catch (Exception e) {
            promise.tryFailure(e);
            if (logger.isErrorEnabled()) {
                logger.error("Failed to create topic, topic={}", topic, e);
            }
            return;
        }
        promise.trySuccess(null);
    }

    public void delTopic(String topic, Promise<Void> promise) {
        if (StringUtils.isNullOrEmpty(topic)) {
            promise.tryFailure(new IllegalArgumentException("Topic cannot be empty"));
        }

        this.cache.invalidate(topic);
    }

    public Set<Partition> loadAll(List<String> topics) throws Exception {
        Set<Partition> partitions = new HashSet<>();
        if (topics == null || topics.isEmpty()) {
            Iterator<Set<Partition>> iterator = cache.asMap().values().stream().iterator();
            while (iterator.hasNext()) {
                partitions.addAll(iterator.next());
            }
        } else {
            for (String topic : topics) {
                Set<Partition> sets = this.cache.get(topic);
                if (sets == null || sets.isEmpty()) {
                    continue;
                }
                partitions.addAll(sets);
            }
        }
        return partitions;
    }
}
