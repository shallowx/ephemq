package org.meteor.proxy.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.curator.shaded.com.google.common.hash.HashFunction;
import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.MessageLedger;
import org.meteor.common.Node;
import org.meteor.common.TopicConfig;
import org.meteor.common.TopicPartition;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogCoordinator;
import org.meteor.listener.TopicListener;
import org.meteor.coordinatior.Coordinator;
import org.meteor.net.ServiceProcessor;
import org.meteor.proxy.MeteorProxy;
import org.meteor.proxy.coordinatior.LedgerSyncCoordinator;
import org.meteor.proxy.coordinatior.ProxyClusterCoordinator;
import org.meteor.proxy.coordinatior.ProxyTopicCoordinator;
import org.meteor.proxy.coordinatior.ProxyCoordinator;
import org.meteor.proxy.internal.ProxyLog;
import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.remote.RemoteException;
import org.meteor.remote.invoke.InvokeAnswer;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.server.*;
import org.meteor.remote.util.NetworkUtils;
import org.meteor.remote.util.ProtoBufUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ProxyServiceProcessor extends ServiceProcessor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    private static final int MIN_REPLICA_LIMIT = 2;
    private final LedgerSyncCoordinator syncCoordinator;
    private final ProxyClusterCoordinator proxyClusterCoordinator;
    private final int subscribeThreshold;
    private final ProxyServerConfig serverConfiguration;

    public ProxyServiceProcessor(ProxyServerConfig config, Coordinator coordinator) {
        super(config.getCommonConfig(), config.getNetworkConfig(), coordinator);
        if (coordinator instanceof ProxyCoordinator) {
            this.syncCoordinator = ((ProxyCoordinator) coordinator).getLedgerSyncCoordinator();
            this.proxyClusterCoordinator = (ProxyClusterCoordinator)coordinator.getClusterCoordinator();
        } else {
            this.syncCoordinator = null;
            this.proxyClusterCoordinator = null;
        }
        this.serverConfiguration = config;
        this.subscribeThreshold = config.getProxyConfiguration().getProxyHeavyLoadSubscriberThreshold();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        final int length = data.readableBytes();
        try {
            switch (command) {
                case QUERY_CLUSTER_INFOS -> processQueryClusterInfo(channel, command, data, answer);
                case QUERY_TOPIC_INFOS -> processQueryTopicInfos(channel, command, data, answer);
                case REST_SUBSCRIBE -> processRestSubscription(channel, command, data, answer);
                case ALTER_SUBSCRIBE -> processAlterSubscription(channel, command, data, answer);
                case CLEAN_SUBSCRIBE -> processCleanSubscription(channel, command, data, answer);
                case SYNC_LEDGER -> processSyncLedger(channel, command, data, answer);
                case UNSYNC_LEDGER -> processUnSyncLedger(channel, command, data,answer);
                default -> {
                    logger.warn("<{}> command unsupported, code={}, length={}", NetworkUtils.switchAddress(channel), command, length);
                    if (answer != null) {
                        String error = "Command[" + command + "] unsupported, length=" + length;
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, error));
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("<{}> process error, code={}, length={}", NetworkUtils.switchAddress(channel), command, length);
            if (answer != null) {
                answer.failure(t);
            }
        }
    }

    @Override
    protected void processSyncLedger(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        LogCoordinator logCoordinator = coordinator.getLogCoordinator();
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            SyncRequest request = ProtoBufUtils.readProto(data, SyncRequest.parser());
            commandExecutor.submit(() -> {
                try {
                    Promise<SyncResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<SyncResponse>>) f -> {
                       if (f.isSuccess()) {
                           try {
                               if (answer != null) {
                                   SyncResponse response = f.getNow();
                                   answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), response));
                               }
                           } catch (Throwable t) {
                               processFailed("process sync ledger failed", command, channel, answer, t);
                           }
                       } else {
                           processFailed("process sync ledger failed", command, channel, answer, f.cause());
                       }
                       recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });

                    int ledger = request.getLedger();
                    int epoch = request.getEpoch();
                    long index = request.getIndex();
                    String topic = request.getTopic();
                    MessageLedger messageLedger = syncCoordinator.getMessageLedger(topic, ledger);
                    ProxyLog log = getLog(logCoordinator, ledger, messageLedger);
                    ClientChannel syncChannel = syncCoordinator.getSyncChannel(messageLedger);
                    log.syncAndChunkSubscribe(syncChannel, epoch, index,channel, promise);
                } catch (Throwable t) {
                    processFailed("process sync ledger failed", command, channel, answer, t);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        }catch (Throwable t) {
            processFailed("process sync ledger failed", command, channel, answer, t);
            recordCommand(command, bytes, System.nanoTime() - time, false);
        }
    }

    private ProxyLog getLog(LogCoordinator logCoordinator, int ledger, MessageLedger messageLedger) {
        return (ProxyLog) logCoordinator.getOrInitLog(ledger, _ledger -> {
            TopicConfig topicConfig = new TopicConfig(
                    serverConfiguration.getSegmentConfig().getSegmentRollingSize(),
                    serverConfiguration.getSegmentConfig().getSegmentRetainLimit(),
                    serverConfiguration.getSegmentConfig().getSegmentRetainTime(),
                    true
            );
            TopicPartition topicPartition = new TopicPartition(messageLedger.topic(), messageLedger.partition());
            Log log = new ProxyLog(serverConfiguration, topicPartition, ledger, 0, coordinator, topicConfig);
            for (TopicListener listener : coordinator.getTopicCoordinator().getTopicListener()) {
                listener.onPartitionInit(topicPartition, ledger);
            }
            log.start(null);
            return log;
        });
    }

    @Override
    protected void processQueryTopicInfos(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            QueryTopicInfoRequest request = ProtoBufUtils.readProto(data, QueryTopicInfoRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    List<Node> clusterUpNodes = proxyClusterCoordinator.getClusterUpNodes();
                    if (clusterUpNodes == null || clusterUpNodes.isEmpty()) {
                        throw new IllegalStateException("cluster node is empty");
                    }

                    QueryTopicInfoResponse.Builder newResponse = QueryTopicInfoResponse.newBuilder();
                    ProxyTopicCoordinator topicCoordinator = (ProxyTopicCoordinator) coordinator.getTopicCoordinator();
                    Map<String, TopicInfo> topicInfoMap = topicCoordinator.acquireTopicMetadata(request.getTopicNamesList());
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
                    if (answer != null) {
                        answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), newResponse.build()));
                    }
                    recordCommand(command, bytes, System.nanoTime() -time, true);
                }catch (Exception e) {
                    processFailed("process sync ledger failed", command, channel, answer, e);
                    recordCommand(command, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("process sync ledger failed", command, channel, answer, t);
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
        for (Node node : proxyClusterCoordinator.getClusterUpNodes()) {
            if (node == null) {
                continue;
            }
            String nodeId = node.getId();
            Map<Integer, Integer> ledgerThroughput = node.getLedgerThroughput();
            Integer throughput = ledgerThroughput == null ? null : ledgerThroughput.get(ledger);
            int throughputValue = throughput == null || throughput < 0 ? 0: throughput;
            allThroughput += throughputValue;
            if (node.getState().equals("UP")) {
                nodes.put(nodeId, throughputValue);
            }
        }

        int replicaCount = Math.max(MIN_REPLICA_LIMIT, allThroughput / subscribeThreshold + 1);
        String token = topic + "*" + ledger;
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

        String host = ((InetSocketAddress)socketAddress).getHostString();
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
        List<String> routeNodes = proxyClusterCoordinator.route2Nodes(token, replicaCount);
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
            if (selectNodes.size() >= replicaCount){
                return selectNodes;
            }
        }
        return selectNodes;
    }

    @Override
    protected void processUnSyncLedger(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CancelSyncRequest request = ProtoBufUtils.readProto(data, CancelSyncRequest.parser());
            int ledger = request.getLedger();
            Promise<Void> promise = commandExecutor.newPromise();
            promise.addListener(f -> {
                if (f.isSuccess()) {
                    try {
                        if (answer != null) {
                            CancelSyncResponse response = CancelSyncResponse.newBuilder().build();
                            answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), response));
                        }
                    } catch (Throwable t) {
                        processFailed("process un-sync ledger failed", command, channel, answer, t);
                    }
                } else {
                    processFailed("process un-sync ledger failed", command, channel, answer, f.cause());
                }
                recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
            });
            LogCoordinator logCoordinator = coordinator.getLogCoordinator();
            Log log = logCoordinator.getLog(ledger);
            if (log == null) {
                promise.trySuccess(null);
                return;
            }
            log.subscribeSynchronize(channel, promise);
        } catch (Throwable t) {
            processFailed("process un-sync ledger failed", command, channel, answer, t);
            recordCommand(command, bytes, System.nanoTime() - time,false);
        }
    }

    @Override
    protected void processRestSubscription(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        LogCoordinator logCoordinator = coordinator.getLogCoordinator();
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            ResetSubscribeRequest request = ProtoBufUtils.readProto(data, ResetSubscribeRequest.parser());
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
                           if (answer != null) {
                               ResetSubscribeResponse response = ResetSubscribeResponse.newBuilder().build();
                               answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), response));
                           }
                       } else {
                           processFailed("process rest subscribe failed", command, channel, answer, f.cause());
                       }
                       recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                   });
                   MessageLedger messageLedger = syncCoordinator.getMessageLedger(topic, ledger);
                   ProxyLog log = getLog(logCoordinator, ledger, messageLedger);
                   ClientChannel syncChannel = syncCoordinator.getSyncChannel(messageLedger);
                   log.syncAndResetSubscribe(syncChannel, epoch, index, channel, markers, promise);
               } catch (Exception e) {
                   processFailed("process rest subscribe failed", command, channel, answer, e);
                   recordCommand(command, bytes, System.nanoTime() - time,false);
               }
            });
        } catch (Exception e) {
            processFailed("process rest subscribe failed", command, channel, answer, e);
            recordCommand(command, bytes, System.nanoTime() - time,false);
        }
    }

    @Override
    protected void processAlterSubscription(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        LogCoordinator logCoordinator = coordinator.getLogCoordinator();
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            AlterSubscribeRequest request = ProtoBufUtils.readProto(data, AlterSubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    int ledger = request.getLedger();
                    IntList appendMarkers = convertMarkers(request.getAppendMarkers());
                    IntList deleteMarkers = convertMarkers(request.getDeleteMarkers());
                    Promise<Integer> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
                                AlterSubscribeResponse response = AlterSubscribeResponse.newBuilder().build();
                                answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            processFailed("process alter subscribe failed", command, channel, answer, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });
                    Log log = logCoordinator.getLog(ledger);
                    if (log == null) {
                        promise.tryFailure(new IllegalStateException("alter subscribe failed, since log does not exist"));
                        return;
                    }
                    log.alterSubscribe(channel, appendMarkers, deleteMarkers, promise);
                } catch (Exception e) {
                    processFailed("process alter subscribe failed", command, channel, answer, e);
                    recordCommand(command, bytes, System.nanoTime() - time,false);
                }
            });
        } catch (Exception e) {
            processFailed("process alter subscribe failed", command, channel, answer, e);
            recordCommand(command, bytes, System.nanoTime() - time,false);
        }
    }
    @Override
    protected void processCleanSubscription(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        LogCoordinator logCoordinator = coordinator.getLogCoordinator();
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CleanSubscribeRequest request = ProtoBufUtils.readProto(data, CleanSubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    Promise<Boolean> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                    promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
                                CleanSubscribeResponse response = CleanSubscribeResponse.newBuilder().build();
                                answer.success(ProtoBufUtils.proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            processFailed("process clean subscribe failed", command, channel, answer, f.cause());
                        }
                        recordCommand(command, bytes, System.nanoTime() - time, f.isSuccess());
                    });
                    logCoordinator.cleanSubscribe(channel, request.getLedger(), promise);
                } catch (Exception e) {
                    processFailed("process clean subscribe failed", command, channel, answer, e);
                    recordCommand(command, bytes, System.nanoTime() - time,false);
                }
            });
        } catch (Exception e) {
            processFailed("process rest subscribe failed", command, channel, answer, e);
            recordCommand(command, bytes, System.nanoTime() - time,false);
        }
    }
}
