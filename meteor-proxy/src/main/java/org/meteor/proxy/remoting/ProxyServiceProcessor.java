package org.meteor.proxy.remoting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.MessageLedger;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.TopicListener;
import org.meteor.proxy.core.ProxyLog;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.proxy.support.LedgerSyncCoordinator;
import org.meteor.proxy.support.ProxyClusterManager;
import org.meteor.proxy.support.ProxyManager;
import org.meteor.proxy.support.ProxyTopicCoordinator;
import org.meteor.remote.exception.RemotingException;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.server.AlterSubscribeRequest;
import org.meteor.remote.proto.server.AlterSubscribeResponse;
import org.meteor.remote.proto.server.CancelSyncRequest;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.CleanSubscribeRequest;
import org.meteor.remote.proto.server.CleanSubscribeResponse;
import org.meteor.remote.proto.server.QueryTopicInfoRequest;
import org.meteor.remote.proto.server.QueryTopicInfoResponse;
import org.meteor.remote.proto.server.ResetSubscribeRequest;
import org.meteor.remote.proto.server.ResetSubscribeResponse;
import org.meteor.remote.proto.server.SyncRequest;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.remote.util.ProtoBufUtil;
import org.meteor.remoting.ServiceProcessor;
import org.meteor.support.Manager;

class ProxyServiceProcessor extends ServiceProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyServiceProcessor.class);

    private static final int MIN_REPLICA_LIMIT = 2;
    private final LedgerSyncCoordinator syncCoordinator;
    private final ProxyClusterManager proxyClusterManager;
    private final int subscribeThreshold;
    private final ProxyServerConfig serverConfiguration;

    public ProxyServiceProcessor(ProxyServerConfig config, Manager manager) {
        super(config.getCommonConfig(), config.getNetworkConfig(), manager);
        if (manager instanceof ProxyManager) {
            this.syncCoordinator = ((ProxyManager) manager).getLedgerSyncCoordinator();
            this.proxyClusterManager = (ProxyClusterManager) manager.getClusterManager();
        } else {
            this.syncCoordinator = null;
            this.proxyClusterManager = null;
        }
        this.serverConfiguration = config;
        this.subscribeThreshold = config.getProxyConfiguration().getProxyHeavyLoadSubscriberThreshold();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        final int length = data.readableBytes();
        try {
            switch (command) {
                case QUERY_CLUSTER_INFOS -> processQueryClusterInfo(channel, command, data, feedback);
                case QUERY_TOPIC_INFOS -> processQueryTopicInfos(channel, command, data, feedback);
                case REST_SUBSCRIBE -> processRestSubscription(channel, command, data, feedback);
                case ALTER_SUBSCRIBE -> processAlterSubscription(channel, command, data, feedback);
                case CLEAN_SUBSCRIBE -> processCleanSubscription(channel, command, data, feedback);
                case SYNC_LEDGER -> processSyncLedger(channel, command, data, feedback);
                case CANCEL_SYNC_LEDGER -> processUnSyncLedger(channel, command, data, feedback);
                default -> {
                    if (feedback != null) {
                        feedback.failure(RemotingException.of(RemotingException.Failure.UNSUPPORTED_EXCEPTION,
                                "Proxy command[" + command + "] unsupported, length=" + length));
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Proxy command[{}] unsupported, channel={} length={} ", command, NetworkUtil.switchAddress(channel), length);
                    }
                }
            }
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Proxy process error, channel={} code={} length={}", NetworkUtil.switchAddress(channel), command, length);
            }

            if (feedback != null) {
                feedback.failure(t);
            }
        }
    }

    @Override
    protected void processSyncLedger(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            SyncRequest request = ProtoBufUtil.readProto(data, SyncRequest.parser());
            commandExecutor.submit(() -> {
                try {
                    Promise<SyncResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<SyncResponse>>) f -> {
                        if (f.isSuccess()) {
                            try {
                                if (feedback != null) {
                                    SyncResponse response = f.getNow();
                                    feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response));
                                }
                            } catch (Throwable t) {
                                processFailed("Proxy process sync ledger[" + request.getLedger() + "] failed", command, channel, feedback, t);
                            }
                        } else {
                            processFailed("Proxy process sync ledger[" + request.getLedger() + "] failed", command, channel, feedback, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });

                    int ledger = request.getLedger();
                    int epoch = request.getEpoch();
                    long index = request.getIndex();
                    String topic = request.getTopic();
                    MessageLedger messageLedger = syncCoordinator.getMessageLedger(topic, ledger);
                    ProxyLog log = getLog(manager.getLogHandler(), ledger, messageLedger);
                    ClientChannel syncChannel = syncCoordinator.getSyncChannel(messageLedger);
                    log.syncAndChunkSubscribe(syncChannel, epoch, index, channel, promise);
                } catch (Throwable t) {
                    processFailed("Proxy process sync ledger[" + request.getLedger() + "] failed", command, channel, feedback, t);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Proxy process sync ledger failed", command, channel, feedback, t);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    private ProxyLog getLog(LogHandler logCoordinator, int ledger, MessageLedger messageLedger) {
        return (ProxyLog) logCoordinator.getOrInitLog(ledger, _ledger -> {
            TopicConfig topicConfig = new TopicConfig(
                    serverConfiguration.getSegmentConfig().getSegmentRollingSize(),
                    serverConfiguration.getSegmentConfig().getSegmentRetainLimit(),
                    serverConfiguration.getSegmentConfig().getSegmentRetainTimeMilliseconds(),
                    true
            );
            TopicPartition topicPartition = new TopicPartition(messageLedger.topic(), messageLedger.partition());
            Log log = new ProxyLog(serverConfiguration, topicPartition, ledger, 0, manager, topicConfig);
            for (TopicListener listener : Objects.requireNonNull(manager.getTopicCoordinator().getTopicListener())) {
                listener.onPartitionInit(topicPartition, ledger);
            }
            log.start(null);
            return log;
        });
    }

    @Override
    protected void processQueryTopicInfos(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            QueryTopicInfoRequest request = ProtoBufUtil.readProto(data, QueryTopicInfoRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    List<Node> clusterUpNodes = proxyClusterManager.getClusterReadyNodes();
                    if (clusterUpNodes == null || clusterUpNodes.isEmpty()) {
                        throw new IllegalStateException("Proxy cluster node is empty");
                    }

                    QueryTopicInfoResponse.Builder newResponse = QueryTopicInfoResponse.newBuilder();
                    ProxyTopicCoordinator topicCoordinator = (ProxyTopicCoordinator) manager.getTopicCoordinator();
                    Map<String, TopicInfo> topicInfoMap = topicCoordinator.getTopicMetadata(request.getTopicNamesList());
                    Map<String, TopicInfo> newTopicInfoMap = new Object2ObjectOpenHashMap<>();
                    if (topicInfoMap != null && !topicInfoMap.isEmpty()) {
                        for (Map.Entry<String, TopicInfo> entry : topicInfoMap.entrySet()) {
                            String topic = entry.getKey();
                            TopicInfo topicInfo = entry.getValue();
                            if (topicInfo == null) {
                                continue;
                            }

                            Int2ObjectOpenHashMap<PartitionMetadata> newPartitions = new Int2ObjectOpenHashMap<>();
                            for (Map.Entry<Integer, PartitionMetadata> partitionMetadataEntry : topicInfo.getPartitionsMap().entrySet()) {
                                int partition = partitionMetadataEntry.getKey();
                                PartitionMetadata partitionMetadata = partitionMetadataEntry.getValue();
                                PartitionMetadata.Builder newPartitionMetadata = PartitionMetadata.newBuilder();
                                newPartitionMetadata.setTopicName(partitionMetadata.getTopicName());
                                newPartitionMetadata.setId(partitionMetadata.getId());
                                newPartitionMetadata.setEpoch(partitionMetadata.getEpoch());
                                newPartitionMetadata.setVersion(partitionMetadata.getVersion());

                                int ledger = partitionMetadata.getLedger();
                                newPartitionMetadata.setLedger(ledger);
                                NavigableMap<String, Integer> replicas = calculateReplicas(channel, topic, ledger);
                                String leader = selectLeader(replicas);
                                newPartitionMetadata.setLeaderNodeId(leader);
                                newPartitionMetadata.addAllReplicaNodeIds(replicas.keySet());
                                newPartitions.put(partition, newPartitionMetadata.build());
                            }

                            TopicInfo newTopicInfo = TopicInfo.newBuilder()
                                    .putAllPartitions(newPartitions)
                                    .setTopic(topicInfo.getTopic())
                                    .build();
                            newTopicInfoMap.put(topic, newTopicInfo);
                        }
                    }
                    newResponse.putAllTopicInfos(newTopicInfoMap);
                    if (feedback != null) {
                        feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), newResponse.build()));
                    }
                    recordCommand(command, bytes, System.nanoTime() - time, true);
                } catch (Exception e) {
                    processFailed("Proxy process sync ledger failed", command, channel, feedback, e);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Proxy process sync ledger failed", command, channel, feedback, t);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    private String selectLeader(NavigableMap<String, Integer> replicas) {
        if (replicas.isEmpty()) {
            return null;
        }

        if (replicas.size() == 1) {
            return replicas.firstKey();
        }

        List<String> nodes = new ArrayList<>(replicas.size());
        int limit = subscribeThreshold + subscribeThreshold >> 1;
        for (Map.Entry<String, Integer> entry : replicas.entrySet()) {
            int throughput = entry.getValue();
            if (throughput < limit) {
                nodes.add(entry.getKey());
            }
        }

        if (nodes.isEmpty()) {
            nodes.addAll(replicas.keySet());
        }
        return nodes.size() == 1 ? nodes.get(0) : nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
    }

    private NavigableMap<String, Integer> calculateReplicas(Channel channel, String topic, int ledger) {
        int allThroughput = 0;
        NavigableMap<String, Integer> nodes = new TreeMap<>();
        for (Node node : proxyClusterManager.getClusterReadyNodes()) {
            if (node == null) {
                continue;
            }
            String nodeId = node.getId();
            Map<Integer, Integer> ledgerThroughput = node.getLedgerThroughput();
            Integer throughput = ledgerThroughput == null ? null : ledgerThroughput.get(ledger);
            int throughputValue = throughput == null || throughput < 0 ? 0 : throughput;
            allThroughput += throughputValue;
            if (node.getState().equals("UP")) {
                nodes.put(nodeId, throughputValue);
            }
        }

        int replicaCount = Math.max(MIN_REPLICA_LIMIT, allThroughput / subscribeThreshold + 1);
        String token = topic + "#" + ledger;
        NavigableMap<String, Integer> ledgerNodes = selectLedgerNodes(replicaCount, token, nodes);
        return selectChannelNodes(channel, token, ledgerNodes);
    }

    private NavigableMap<String, Integer> selectChannelNodes(Channel channel, String token, NavigableMap<String, Integer> nodes) {
        if (nodes.size() <= MIN_REPLICA_LIMIT) {
            return nodes;
        }
        SocketAddress socketAddress = channel.remoteAddress();
        if (!(socketAddress instanceof InetSocketAddress)) {
            return nodes;
        }

        String host = ((InetSocketAddress) socketAddress).getHostString();
        if (host == null) {
            return nodes;
        }
        HashFunction function = Hashing.murmur3_32();
        String baseToken = token + "#" + host;
        NavigableMap<Integer, NavigableSet<String>> hashMap = new TreeMap<>();
        for (String node : nodes.keySet()) {
            String key = baseToken + "@" + node;
            int hash = function.hashUnencodedChars(key).asInt();
            hashMap.computeIfAbsent(hash, k -> new TreeSet<>()).add(node);
        }

        NavigableMap<String, Integer> channelNodes = new TreeMap<>();
        int baseHash = function.hashUnencodedChars(baseToken).asInt();
        Map.Entry<Integer, NavigableSet<String>> tempEntry = hashMap.higherEntry(baseHash);
        for (int i = 0; i < hashMap.size(); i++) {
            if (tempEntry == null) {
                tempEntry = hashMap.firstEntry();
            }

            NavigableSet<String> tempNodes = tempEntry.getValue();
            for (String tempNode : tempNodes) {
                if (!channelNodes.containsKey(tempNode)) {
                    channelNodes.put(tempNode, nodes.get(tempNode));
                    if (channelNodes.size() >= MIN_REPLICA_LIMIT) {
                        return channelNodes;
                    }
                }
            }

            tempEntry = hashMap.higherEntry(tempEntry.getKey());
        }
        return channelNodes;
    }

    private NavigableMap<String, Integer> selectLedgerNodes(int replicaCount, String token, NavigableMap<String, Integer> nodes) {
        if (nodes.size() <= replicaCount) {
            return nodes;
        }
        NavigableMap<String, Integer> selectNodes = new TreeMap<>();
        Set<String> routeNodes = proxyClusterManager.route2Nodes(token, replicaCount);
        for (String node : routeNodes) {
            Integer throughput = nodes.get(node);
            selectNodes.put(node, throughput == null ? 0 : throughput);
            if (selectNodes.size() >= replicaCount) {
                return selectNodes;
            }
        }

        String[] nodeIds = nodes.keySet().toArray(new String[0]);
        int index = (token.hashCode() & 0x7ffffff) % nodeIds.length;
        for (int i = 0; i < nodeIds.length; i++) {
            String nodeId = nodeIds[(index + i) % nodeIds.length];
            if (selectNodes.containsKey(nodeId)) {
                continue;
            }

            selectNodes.put(nodeId, nodes.get(nodeId));
            if (selectNodes.size() >= replicaCount) {
                return selectNodes;
            }
        }
        return selectNodes;
    }

    @Override
    protected void processUnSyncLedger(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CancelSyncRequest request = ProtoBufUtil.readProto(data, CancelSyncRequest.parser());
            int ledger = request.getLedger();
            Promise<Void> promise = commandExecutor.newPromise();
            promise.addListener(f -> {
                if (f.isSuccess()) {
                    try {
                        if (feedback != null) {
                            CancelSyncResponse response = CancelSyncResponse.newBuilder().build();
                            feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response));
                        }
                    } catch (Throwable t) {
                        processFailed("Proxy process un-sync ledger failed", command, channel, feedback, t);
                    }
                } else {
                    processFailed("Proxy process un-sync ledger failed", command, channel, feedback, f.cause());
                }
                recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
            });
            LogHandler logCoordinator = manager.getLogHandler();
            Log log = logCoordinator.getLog(ledger);
            if (log == null) {
                promise.trySuccess(null);
                return;
            }
            log.subscribeSynchronize(channel, promise);
        } catch (Throwable t) {
            processFailed("Proxy process un-sync ledger failed", command, channel, feedback, t);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    @Override
    protected void processRestSubscription(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            ResetSubscribeRequest request = ProtoBufUtil.readProto(data, ResetSubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    String topic = request.getTopic();
                    int ledger = request.getLedger();
                    int epoch = request.getEpoch();
                    long index = request.getIndex();
                    IntList markers = convertMarkers(request.getMarkers());
                    Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                        if (f.isSuccess()) {
                            if (feedback != null) {
                                ResetSubscribeResponse response = ResetSubscribeResponse.newBuilder().build();
                                feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            processFailed("Proxy process rest subscribe failed", command, channel, feedback, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });
                    MessageLedger messageLedger = syncCoordinator.getMessageLedger(topic, ledger);
                    ProxyLog log = getLog(manager.getLogHandler(), ledger, messageLedger);
                    ClientChannel syncChannel = syncCoordinator.getSyncChannel(messageLedger);
                    log.syncAndResetSubscribe(syncChannel, epoch, index, channel, markers, promise);
                } catch (Exception e) {
                    processFailed("Proxy process rest subscribe failed", command, channel, feedback, e);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Exception e) {
            processFailed("Proxy process rest subscribe failed", command, channel, feedback, e);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    @Override
    protected void processAlterSubscription(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            AlterSubscribeRequest request = ProtoBufUtil.readProto(data, AlterSubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    int ledger = request.getLedger();
                    IntList appendMarkers = convertMarkers(request.getAppendMarkers());
                    IntList deleteMarkers = convertMarkers(request.getDeleteMarkers());
                    Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                        if (f.isSuccess()) {
                            if (feedback != null) {
                                AlterSubscribeResponse response = AlterSubscribeResponse.newBuilder().build();
                                feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            processFailed("Proxy process alter subscribe failed", command, channel, feedback, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });
                    Log log = manager.getLogHandler().getLog(ledger);
                    if (log == null) {
                        promise.tryFailure(new IllegalStateException("Proxy alter subscribe failed, since log does not exist"));
                        return;
                    }
                    log.alterSubscribe(channel, appendMarkers, deleteMarkers, promise);
                } catch (Exception e) {
                    processFailed("Proxy process alter subscribe failed", command, channel, feedback, e);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Exception e) {
            processFailed("Proxy process alter subscribe failed", command, channel, feedback, e);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    @Override
    protected void processCleanSubscription(Channel channel, int command, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CleanSubscribeRequest request = ProtoBufUtil.readProto(data, CleanSubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    Promise<Boolean> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                        if (f.isSuccess()) {
                            if (feedback != null) {
                                CleanSubscribeResponse response = CleanSubscribeResponse.newBuilder().build();
                                feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            processFailed("Proxy process clean subscribe failed", command, channel, feedback, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });
                    manager.getLogHandler().cleanSubscribe(channel, request.getLedger(), promise);
                } catch (Exception e) {
                    processFailed("Proxy process clean subscribe failed", command, channel, feedback, e);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Exception e) {
            processFailed("Proxy process rest subscribe failed", command, channel, feedback, e);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }
}
