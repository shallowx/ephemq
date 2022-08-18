package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.*;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.Server.REGISTER_NODE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class ClusterManager extends AbstractSRaftLog<NodeRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final LoadingCache<String, Set<NodeRecord>> nodeRecordCommitCache;
    private final LoadingCache<String, Set<NodeRecord>> nodeRecordUnCommitCache;
    private final MappedFileApi api;

    public ClusterManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, BrokerManager manager) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config, manager.getController());
        this.api = manager.getMappedFileApi();

        this.nodeRecordCommitCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        return syncFromQuorumLeader(key);
                    }
                });
        this.nodeRecordUnCommitCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    public void start() {
        registerNodeToQuorumLeader();
    }

    private void registerNodeToQuorumLeader() {
        RegisterNodeRequest request = RegisterNodeRequest
                .newBuilder()
                .setCluster(config.getClusterName())
                .setMetadata(NodeMetadata
                        .newBuilder()
                        .setHost(config.getExposedHost())
                        .setPort(config.getExposedPort())
                        .setName(config.getServerId())
                        .build())
                .build();

        ClientChannel clientChannel = pool.acquireHealthyOrNew(controller.getMetadataLeader());

        Promise<RegisterNodeResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<RegisterNodeResponse>>) future -> {
            if (!future.isSuccess()) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to register cluster node, try again later");
                }
            }
        });

        clientChannel.invoker().invoke(REGISTER_NODE, config.getInvokeTimeMs(), promise, request, RegisterNodeResponse.class);
    }

    private Set<NodeRecord> syncFromQuorumLeader(String cluster) {
        if (controller.isQuorumLeader()) {
            NodeRecord record = new NodeRecord(config.getClusterName(), config.getServerId(), switchSocketAddress(config.getExposedHost(), config.getExposedPort()));
            CopyOnWriteArraySet<NodeRecord> nodeRecords = new CopyOnWriteArraySet<>();
            nodeRecords.add(record);

            return nodeRecords;
        }

        try {
            SocketAddress metadataLeader = controller.getMetadataLeader();
            ClientChannel clientChannel = pool.acquireHealthyOrNew(metadataLeader);

            Promise<QueryClusterNodeResponse> promise = newImmediatePromise();

            QueryClusterNodeRequest request = QueryClusterNodeRequest.newBuilder().setCluster(cluster).build();
            clientChannel.invoker().invoke(ProcessCommand.Server.FETCH_CLUSTER_RECORD, config.getInvokeTimeMs(), promise, request, QueryTopicInfoResponse.class);

            QueryClusterNodeResponse response = promise.get(config.getInvokeTimeMs(), TimeUnit.MILLISECONDS);
            List<NodeMetadata> nodes = response.getNodesList();

            if (nodes.isEmpty()) {
                return null;
            }

            return nodes.stream()
                    .map(nodeMetadata -> new NodeRecord(cluster, nodeMetadata.getName(), switchSocketAddress(nodeMetadata.getHost(), nodeMetadata.getPort())))
                    .collect(Collectors.toCollection(
                            CopyOnWriteArraySet::new
                    ));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<NodeRecord> getClusterInfo(String cluster) {
        Set<NodeRecord> nodeRecords = nodeRecordCommitCache.get(cluster);
        if (nodeRecords.isEmpty()) {
            return null;
        }
        return nodeRecords;
    }

    @Override
    protected CommitRecord<NodeRecord> doPrepareCommit(NodeRecord nodeRecord, CommitType type) {

        return null;
    }

    @Override
    protected void doPostCommit(NodeRecord nodeRecord, CommitType type) {

    }
}
