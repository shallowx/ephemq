package org.shallow.metadata.sraft;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.PartitionRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.server.*;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.Server.REGISTER_NODE;
import static org.shallow.util.NetworkUtil.*;

public final class SRaftQuorumVoterClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftQuorumVoterClient.class);

    private final ClientConfig clientConfig;
    private ShallowChannelPool pool;

    public SRaftQuorumVoterClient(String name, ClientConfig config) {
        super(name, config);
        this.clientConfig = config;
    }

    @Override
    public void start() {
        super.start();
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
    }

    public Set<PartitionRecord> queryTopicInfo(String topic, SocketAddress leader) {
        try {
            ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

            QueryTopicInfoRequest request = QueryTopicInfoRequest
                    .newBuilder()
                    .addAllTopic(List.of(topic))
                    .build();
            Promise<QueryTopicInfoResponse> promise = NetworkUtil.newImmediatePromise();
            clientChannel.invoker().invoke(ProcessCommand.Server.FETCH_TOPIC_RECORD, clientConfig.getInvokeExpiredMs(), promise, request, QueryTopicInfoResponse.class);

            QueryTopicInfoResponse response = promise.get(clientConfig.getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            Map<String, TopicMetadata> topicsMap = response.getTopicsMap();

            if (topicsMap.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Query topic<{}> information is null");
                }
                return null;
            }

            Map<Integer, PartitionMetadata> partitionsMap = topicsMap
                    .entrySet()
                    .stream()
                    .iterator()
                    .next()
                    .getValue()
                    .getPartitionsMap();

            if (partitionsMap.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Query topic<{}> partition information is null");
                }
                return null;
            }

            return partitionsMap.values().stream()
                    .map(metadata -> new PartitionRecord(metadata.getId(), metadata.getLatency(), metadata.getLeader(), metadata.getReplicasList()))
                    .collect(Collectors.toCollection(CopyOnWriteArraySet::new));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public Set<NodeRecord> queryClusterInfo(String cluster, SocketAddress leader) {
        try {
            ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

            Promise<QueryClusterNodeResponse> promise = newImmediatePromise();

            QueryClusterNodeRequest request = QueryClusterNodeRequest.newBuilder().setCluster(cluster).build();
            clientChannel.invoker().invoke(ProcessCommand.Server.FETCH_CLUSTER_RECORD, clientConfig.getInvokeExpiredMs(), promise, request, QueryTopicInfoResponse.class);

            QueryClusterNodeResponse response = promise.get(clientConfig.getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            List<NodeMetadata> nodes = response.getNodesList();

            if (nodes.isEmpty()) {
                return null;
            }

            return nodes.stream()
                    .map(nodeMetadata -> new NodeRecord(cluster, nodeMetadata.getName(), nodeMetadata.getState(), switchSocketAddress(nodeMetadata.getHost(), nodeMetadata.getPort())))
                    .collect(Collectors.toCollection(
                            CopyOnWriteArraySet::new
                    ));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void registerNodeToQuorumLeader(SocketAddress leader, String cluster, String host, int port, String serverId) {
        RegisterNodeRequest request = RegisterNodeRequest
                .newBuilder()
                .setCluster(cluster)
                .setMetadata(NodeMetadata
                        .newBuilder()
                        .setHost(host)
                        .setPort(port)
                        .setName(serverId)
                        .build())
                .build();

        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

        Promise<RegisterNodeResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<RegisterNodeResponse>>) future -> {
            if (!future.isSuccess()) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to register cluster node, try again later");
                }
            }
        });

        clientChannel.invoker().invoke(REGISTER_NODE, clientConfig.getInvokeExpiredMs(), promise, request, RegisterNodeResponse.class);
    }
}
