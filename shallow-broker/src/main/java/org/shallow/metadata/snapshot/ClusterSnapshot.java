package org.shallow.metadata.snapshot;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.ClientChannel;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.MappingFileProcessor;
import org.shallow.metadata.raft.LeaderElector;
import org.shallow.metadata.raft.RaftQuorumClient;
import org.shallow.metadata.raft.RaftVoteProcessor;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.QueryClusterNodeRequest;
import org.shallow.proto.server.QueryClusterNodeResponse;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class ClusterSnapshot {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterSnapshot.class);

    private final MappingFileProcessor processor;
    private final BrokerConfig config;
    private final LoadingCache<String, Set<NodeRecord>> clusters;
    private final LeaderElector leaderElector;
    private final ShallowChannelPool pool;

    public ClusterSnapshot(MappingFileProcessor processor, BrokerConfig config, RaftVoteProcessor voteProcessor, RaftQuorumClient client) {
        this.processor = processor;
        this.config = config;
        this.leaderElector = voteProcessor.getLeaderElector();
        this.pool = client.getChanelPool();

        this.clusters = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Set<NodeRecord> load(String key) throws Exception {
                return applyFromNameServer(key);
            }
        });
    }

    public void start() throws Exception {
        registerNode(config.getClusterName(), config.getServerId(), config.getExposedHost(), config.getExposedPort(), NodeRecord.UP, null);
    }

    private Set<NodeRecord> applyFromNameServer(String cluster) throws Exception {
        SocketAddress leaderAddress = leaderElector.getAddress();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leaderAddress);

        QueryClusterNodeRequest request = QueryClusterNodeRequest
                .newBuilder()
                .setCluster(cluster)
                .build();

        Promise<QueryClusterNodeResponse> promise = newImmediatePromise();
        clientChannel.invoker().invoke(ProcessCommand.Server.FETCH_CLUSTER_RECORD, config.getInvokeTimeMs(), promise, request, QueryClusterNodeResponse.class);

        QueryClusterNodeResponse response = promise.get(config.getInvokeTimeMs(), TimeUnit.MILLISECONDS);
        List<NodeMetadata> nodesList = response.getNodesList();
        if (nodesList.isEmpty()) {
            return null;
        }

        Set<NodeRecord> records = new CopyOnWriteArraySet<>();
        for (NodeMetadata metadata : nodesList) {
            NodeRecord record = NodeRecord.newBuilder()
                    .name(metadata.getName())
                    .cluster(metadata.getCluster())
                    .socketAddress(switchSocketAddress(metadata.getHost(), metadata.getPort()))
                    .state(metadata.getState())
                    .build();

            records.add(record);
        }
        return records;
    }

    public void registerNode(String cluster, String name, String host, int port, String state, Promise<Void> promise) {
        Promise<Void> registerPromise = (promise == null) ? newImmediatePromise() : promise;

        try {
            Set<NodeRecord> nodeRecords = applyCollections(cluster);
            SocketAddress socketAddress = switchSocketAddress(host, port);

            NodeRecord nodeRecord = NodeRecord
                    .newBuilder()
                    .cluster(cluster)
                    .name(name)
                    .state(state)
                    .socketAddress(socketAddress)
                    .build();
            if (nodeRecords.isEmpty() || NodeRecord.UN_COMMIT.equals(state)) {
                nodeRecords.add(nodeRecord);
                return;
            }

            if (NodeRecord.UP.equals(state)) {
                for (NodeRecord record : nodeRecords) {
                    if (record.equals(nodeRecord)) {
                        record.setState(NodeRecord.UP);
                    }
                }
            }
            registerPromise.trySuccess(null);
        } catch (Throwable t) {
            registerPromise.tryFailure(t);
        }
    }

    public Set<NodeRecord> getNodeRecord(String cluster) {
        if (cluster == null || cluster.length() == 0) {
            cluster = config.getClusterName();
        }
        return clusters.get(cluster);
    }

    public Set<NodeRecord> checkIfEnough(boolean enough, int latencies) {
        Set<NodeRecord> sets = exclude();
        if (sets.isEmpty()) {
            throw new IllegalArgumentException("Not enough cluster node, and the cluster is empty");
        }

        if (enough) {
            if (sets.size() < latencies) {
                throw new IllegalArgumentException(String.format("Not enough cluster node, and cluster node size:%d, but expect latency:%d", sets.size(), latencies));
            }
        }
        return sets;
    }

    private Set<NodeRecord> applyCollections(String cluster) {
        Set<NodeRecord> nodeRecords = clusters.get(cluster);
        if (nodeRecords == null) {
            nodeRecords = new CopyOnWriteArraySet<>();
            clusters.put(cluster, nodeRecords);
        }
        return nodeRecords;
    }

    private Set<NodeRecord> exclude() {
        Set<NodeRecord> nodeRecords = getNodeRecord(config.getClusterName());

        if (config.isStandAlone()) {
            return nodeRecords;
        }

        return nodeRecords.stream().filter(record -> {
            String name = record.getName();
            return !config.getServerId().equals(name);
        }).collect(Collectors.toSet());
    }
}
