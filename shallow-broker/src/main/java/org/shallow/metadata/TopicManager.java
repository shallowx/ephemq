package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.pool.DefaultFixedChannelPoolFactory;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TopicManager extends AbstractSRaftLog<TopicRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final LoadingCache<String, PartitionRecord> topicCommitRecordCache;
    private final LoadingCache<String, PartitionRecord> topicUnCommitRecordCache;
    private final MappedFileApi api;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, MappedFileApi api) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config);
        this.api = api;
        this.topicCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable PartitionRecord load(String key) throws Exception {
                        return null;
                    }
                });

        this.topicUnCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable PartitionRecord load(String key) throws Exception {
                        return null;
                    }
                });
    }

    @Override
    protected CommitRecord<TopicRecord> doPrepareCommit(TopicRecord topicRecord, CommitType type) {
        topicCommitRecordCache.invalidate(topicRecord.getName());
        topicUnCommitRecordCache.put(topicRecord.getName(), topicRecord.getPartitionRecord());

        return null;
    }

    @Override
    protected void doPostCommit(TopicRecord topicRecord, CommitType type) {
        topicUnCommitRecordCache.invalidate(topicRecord.getName());
        topicCommitRecordCache.put(topicRecord.getName(), topicRecord.getPartitionRecord());
    }
}
