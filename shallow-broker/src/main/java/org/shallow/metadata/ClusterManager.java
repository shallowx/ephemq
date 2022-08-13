package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.pool.DefaultFixedChannelPoolFactory;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusterManager extends AbstractSRaftLog<NodeRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final LoadingCache<String, NodeRecord> nodeRecordCommitCache;
    private final LoadingCache<String, NodeRecord> nodeRecordUnCommitCache;
    private final MappedFileApi api;

    public ClusterManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, MappedFileApi api) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config);
        this.api = api;

        this.nodeRecordCommitCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable NodeRecord load(String key) throws Exception {
                        return null;
                    }
                });
        this.nodeRecordUnCommitCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable NodeRecord load(String key) throws Exception {
                        return null;
                    }
                });
    }

    @Override
    protected CommitRecord<NodeRecord> doPrepareCommit(NodeRecord nodeRecord, CommitType type) {
        nodeRecordUnCommitCache.put(nodeRecord.getName(), nodeRecord);
        nodeRecordCommitCache.invalidate(nodeRecord.getName());

        return null;
    }

    @Override
    protected void doPostCommit(NodeRecord nodeRecord, CommitType type) {
        nodeRecordUnCommitCache.invalidate(nodeRecord.getName());
        nodeRecordCommitCache.put(nodeRecord.getName(), nodeRecord);
    }
}
