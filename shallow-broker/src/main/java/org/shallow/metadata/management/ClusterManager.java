package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.RegisterNodeRequest;
import org.shallow.proto.server.RegisterNodeResponse;
import org.shallow.proto.server.UnRegisterNodeRequest;
import org.shallow.proto.server.UnRegisterNodeResponse;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.metadata.sraft.CommitType.ADD;
import static org.shallow.metadata.sraft.CommitType.REMOVE;
import static org.shallow.util.NetworkUtil.switchSocketAddress;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class ClusterManager extends AbstractSRaftLog<NodeRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final LoadingCache<String, Set<NodeRecord>> commitRecordCache;
    private final LoadingCache<String, Set<NodeRecord>> unCommitRecordCache;
    private final SRaftQuorumVoterClient client;

    public ClusterManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, BrokerManager manager) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config, manager.getController());
        this.client = manager.getQuorumVoterClient();

        this.commitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        return syncFromQuorumLeader(key);
                    }
                });

        this.unCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    public void start() throws Exception {
        registerNode();
    }

    private void registerNode() throws Exception {
        if (controller.isQuorumLeader()) {
            NodeRecord nodeRecord = new NodeRecord(config.getClusterName(), config.getServerId(), NodeRecord.UP, switchSocketAddress(config.getExposedHost(), config.getExposedPort()));
            doPostCommit(nodeRecord, ADD);
            return;
        }

        client.registerNodeToQuorumLeader(controller.getMetadataLeader(), config.getClusterName(), config.getExposedHost(), config.getExposedPort(), config.getServerId());
    }

    private Set<NodeRecord> syncFromQuorumLeader(String cluster) {
        if (controller.isQuorumLeader()) {
            NodeRecord record = new NodeRecord(config.getClusterName(), config.getServerId(), NodeRecord.UP, switchSocketAddress(config.getExposedHost(), config.getExposedPort()));
            CopyOnWriteArraySet<NodeRecord> nodeRecords = new CopyOnWriteArraySet<>();
            nodeRecords.add(record);

            return nodeRecords;
        }

        SocketAddress metadataLeader = controller.getMetadataLeader();
        return client.queryClusterInfo(cluster, metadataLeader);
    }

    public Set<NodeRecord> getClusterInfo(String cluster) {
        Set<NodeRecord> nodeRecords = commitRecordCache.get(cluster);
        if (nodeRecords.isEmpty()) {
            return null;
        }
        return nodeRecords.stream()
                .filter(nodeRecord -> nodeRecord.getState().equals(NodeRecord.UP))
                .collect(Collectors.toSet());
    }

    @Override
    protected CommitRecord<NodeRecord> doPrepareCommit(NodeRecord nodeRecord, CommitType type) {
        CommitRecord<NodeRecord> commitRecord = null;
        switch (type) {
            case ADD -> {
                commitRecord = preRegister(nodeRecord);
            }

            case REMOVE -> {
                commitRecord = preOffLine(nodeRecord);
            }
        }

        return commitRecord;
    }

    private CommitRecord<NodeRecord> preRegister(NodeRecord record) {
        try {
            RegisterNodeRequest.Builder request = RegisterNodeRequest.newBuilder();
            if (!controller.isQuorumLeader()) {
                String nodeSocket = record.getSocketAddress().toString();
                int index = nodeSocket.lastIndexOf(":");
                String host = nodeSocket.substring(0, index);
                int port = Integer.parseInt(nodeSocket.substring(index + 1));
                request.setCluster(record.getCluster())
                        .setMetadata(NodeMetadata
                                .newBuilder()
                                .setName(record.getName())
                                .setHost(host)
                                .setPort(port)
                                .build()
                        );
            }

            RegisterNodeResponse.Builder response = RegisterNodeResponse.newBuilder();
            if (controller.isQuorumLeader()) {
                response.setHost(config.getExposedHost())
                        .setPort(config.getExposedPort())
                        .setServerId(config.getServerId());
            }

            CommitRecord<NodeRecord> commitRecord = new CommitRecord<>(record, ADD, request.build(), response.build());

            String cluster = record.getCluster();
            Set<NodeRecord> nodeRecords = unCommitRecordCache.get(cluster);
            if (isNull(nodeRecords)) {
                nodeRecords = new CopyOnWriteArraySet<>();
            }
            unCommitRecordCache.put(cluster, nodeRecords);

            return commitRecord;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private CommitRecord<NodeRecord> preOffLine(NodeRecord record) {
        String cluster = record.getCluster();

        String nodeSocket = record.getSocketAddress().toString();
        int index = nodeSocket.lastIndexOf(":");
        String host = nodeSocket.substring(0, index);
        int port = Integer.parseInt(nodeSocket.substring(index + 1));

        Set<NodeRecord> nodeRecords = commitRecordCache.get(cluster);
        if (isNotNull(nodeRecords)) {
            nodeRecords.remove(record);
        }

        UnRegisterNodeRequest.Builder request = UnRegisterNodeRequest.newBuilder();
        if (!controller.isQuorumLeader()) {
            request.setCluster(cluster)
                    .setMetadata(NodeMetadata
                            .newBuilder()
                            .setName(record.getName())
                            .setHost(host)
                            .setPort(port)
                            .build())
                    .build();
        }

        UnRegisterNodeResponse response = UnRegisterNodeResponse.newBuilder().build();

        return new CommitRecord<>(record, REMOVE, request.build(), response);
    }


    @Override
    protected void doPostCommit(NodeRecord record, CommitType type) {
        String cluster = record.getCluster();
        switch (type) {
            case ADD -> {
                Set<NodeRecord> unNodeRecords = unCommitRecordCache.get(cluster);
                if (isNotNull(unNodeRecords)) {
                    unNodeRecords.remove(record);
                }

                Set<NodeRecord> nodeRecords = commitRecordCache.get(record.getName());
                if (isNull(nodeRecords)) {
                    nodeRecords = new CopyOnWriteArraySet<>();
                }
                nodeRecords.add(record);
                commitRecordCache.put(record.getName(), nodeRecords);
            }

            case REMOVE -> {
                Set<NodeRecord> unNodeRecords = unCommitRecordCache.get(cluster);
                if (isNotNull(unNodeRecords) && !unNodeRecords.isEmpty()) {
                    unNodeRecords.remove(record);
                }

                Set<NodeRecord> nodeRecords = commitRecordCache.get(cluster);
                if (isNotNull(nodeRecords) && !nodeRecords.isEmpty()) {
                    nodeRecords.remove(record);
                }
            }
        }
    }

    public int getClusterSize(String cluster) {
        return commitRecordCache.asMap().size();
    }
}
