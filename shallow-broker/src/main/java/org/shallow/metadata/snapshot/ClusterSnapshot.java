package org.shallow.metadata.snapshot;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.ClientChannel;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.MappingFileProcessor;
import org.shallow.metadata.MetadataManager;
import org.shallow.metadata.listener.ClusterChangedListener;
import org.shallow.metadata.listener.ClusterListener;
import org.shallow.metadata.sraft.LeaderElector;
import org.shallow.metadata.sraft.RaftQuorumClient;
import org.shallow.metadata.sraft.RaftVoteProcessor;
import org.shallow.network.BrokerConnectionManager;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.elector.UnRegisterNodeRequest;
import org.shallow.proto.elector.UnRegisterNodeResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static org.shallow.util.NetworkUtil.*;

@SuppressWarnings("all")
public class ClusterSnapshot {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterSnapshot.class);

    private final MappingFileProcessor processor;
    private final BrokerConfig config;
    private final LoadingCache<String, Set<NodeRecord>> clusters;
    private final LeaderElector leaderElector;
    private final RaftQuorumClient client;
    private final EventExecutor retryTaskExecutor;
    private final BrokerManager manager;
    private final ClusterListener listener;

    public ClusterSnapshot(MappingFileProcessor processor, BrokerConfig config, RaftVoteProcessor voteProcessor, RaftQuorumClient client, BrokerManager manager) {
        this.processor = processor;
        this.config = config;
        this.leaderElector = voteProcessor.getLeaderElector();
        this.client = client;
        this.manager = manager;
        this.listener = new ClusterChangedListener(manager);
        this.retryTaskExecutor = newEventExecutorGroup(1, "cluster-retry-task").next();
        this.clusters = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Set<NodeRecord> load(String key) throws Exception {
                return applyFromNameServer(key);
            }
        });
    }

    public void start() throws Exception {
        registerNode(config.getClusterName(), config.getServerId(), config.getExposedHost(), config.getExposedPort(), NodeRecord.UP, null);
        retryTaskExecutor.schedule(this::fetchFromQuorumLeader, 1000, TimeUnit.MILLISECONDS);
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
                leaderElector.setVersion(leaderElector.getVersion() + 1);
                return;
            }

            if (NodeRecord.UP.equals(state)) {
                for (NodeRecord record : nodeRecords) {
                    if (record.equals(nodeRecord)) {
                        record.setState(NodeRecord.UP);
                        leaderElector.setVersion(leaderElector.getVersion() + 1);
                    }
                }
            }
            registerPromise.trySuccess(null);
        } catch (Throwable t) {
            registerPromise.tryFailure(t);
        }
    }

    public void unRegisterNode() {
        String cluster = config.getClusterName();
        String serverId = config.getServerId();
        String host = config.getExposedHost();
        int port = config.getExposedPort();

        UnRegisterNodeRequest request = UnRegisterNodeRequest
                .newBuilder()
                .setCluster(config.getClusterName())
                .setMetadata(NodeMetadata
                        .newBuilder()
                        .setName(serverId)
                        .setState(NodeRecord.DOWN)
                        .setHost(host)
                        .setCluster(cluster)
                        .setPort(port)
                        .build())
                .build();

        try {

            if (logger.isWarnEnabled()) {
                logger.warn("This cluster node is going offline, cluster={} name={} host={} port={}", cluster, serverId, host, port);
            }

            ShallowChannelPool chanelPool = client.getChanelPool();
            SocketAddress address = leaderElector.getAddress();
            ClientChannel clientChannel = chanelPool.acquireHealthyOrNew(address);

            CountDownLatch latch = new CountDownLatch(1);
            Promise<UnRegisterNodeResponse> promise = newImmediatePromise();
            promise.addListener(new GenericFutureListener<Future<? super UnRegisterNodeResponse>>() {
                @Override
                public void operationComplete(Future<? super UnRegisterNodeResponse> future) throws Exception {
                    latch.countDown();
                }
            });
            clientChannel.invoker().invoke(ProcessCommand.Server.UN_REGISTER_NODE, config.getInvokeTimeMs(), promise, request, UnRegisterNodeResponse.class);

            if (listener == null) {
                return;
            }
            listener.onServerOffline(serverId, host, port);

            latch.await(60, TimeUnit.SECONDS);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("This clutser node is going offline unregister failure");
            }
        }
    }

    public Set<NodeRecord> getNodeRecord(String cluster) {
        if (cluster == null || cluster.isEmpty()) {
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

    public void fetchFromQuorumLeader() {
        if (config.isStandAlone() || leaderElector.isLeader()) {
            return;
        }

        Promise<Void> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<Void>>) future -> {
            if (!future.isSuccess()) {
                retryTaskExecutor.schedule(this::fetchFromQuorumLeader, 0, TimeUnit.MILLISECONDS);
            }
        });

        try {
            String cluster = config.getClusterName();
            Set<NodeRecord> nodeRecords = applyFromNameServer(cluster);
            clusters.put(cluster, nodeRecords);

            promise.trySuccess(null);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to apply cluster info from quorum leader");
            }
            promise.tryFailure(t);
        }
    }

    private Set<NodeRecord> applyFromNameServer(String cluster) throws Exception {
        SocketAddress leaderAddress = leaderElector.getAddress();
        ClientChannel clientChannel = client.getChanelPool().acquireHealthyOrNew(leaderAddress);

        MetadataManager metadataManager = client.getMetadataManager();
        Set<NodeRecord> nodeRecords = metadataManager.queryNodeRecord(clientChannel);
        if (nodeRecords.isEmpty()) {
            return null;
        }
        return nodeRecords;
    }

    private Set<NodeRecord> applyCollections(String cluster) {
        Set<NodeRecord> nodeRecords = null;
        if (clusters.asMap().isEmpty()) {
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
