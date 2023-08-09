package org.ostara.network;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import io.netty.util.internal.StringUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.common.*;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.listener.APIListener;
import org.ostara.storage.Log;
import org.ostara.management.Manager;
import org.ostara.management.TopicManager;
import org.ostara.management.zookeeper.CorrelationIdConstants;
import org.ostara.remote.RemoteException;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.processor.Processor;
import org.ostara.remote.proto.*;
import org.ostara.remote.proto.server.*;
import org.ostara.remote.util.NetworkUtils;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.ostara.remote.util.ProtoBufUtils.proto2Buf;
import static org.ostara.remote.util.ProtoBufUtils.readProto;

public class ServiceProcessor implements Processor, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceProcessor.class);
    private final CoreConfig config;
    private final Manager manager;
    private final EventExecutor commandExecutor;
    private EventExecutor serviceExecutor;

    @Inject
    public ServiceProcessor(CoreConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        this.serviceExecutor = executor;
        manager.getConnectionManager().add(channel);
        channel.closeFuture().addListener(future -> {
            for (Log log : manager.getLogManager().getLedgerId2LogMap().values()) {
                log.cleanSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
            }
        });
    }

    @Override
    public void process(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        int length = data.readableBytes();
        try {
            switch (code) {
                case SEND_MESSAGE -> processSendMessage(channel, code, data, answer);
                case QUERY_CLUSTER_INFOS -> processQueryClusterInfo(channel, code, data, answer);
                case QUERY_TOPIC_INFOS -> processQueryTopicInfos(channel, code, data, answer);
                case REST_SUBSCRIBE -> processRestSubscription(channel, code, data, answer);
                case ALTER_SUBSCRIBE -> processAlterSubscription(channel, code, data, answer);
                case CLEAN_SUBSCRIBE -> processCleanSubscription(channel, code, data, answer);
                case CREATE_TOPIC -> processCreateTopic(channel, code, data, answer);
                case DELETE_TOPIC -> processDeleteTopic(channel, code, data, answer);
                case MIGRATE_LEDGER -> processMigrateLedger(channel, code, data, answer);
                default -> {
                    if (answer != null) {
                        String error = "Command[" + code + "] unsupported, length=" + length;
                        answer.failure(RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, error));
                    }
                }
            }
        } catch (Throwable t) {
            if (answer != null) {
                answer.failure(t);
            }
        }
    }

    private void processMigrateLedger(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            MigrateLedgerRequest request = readProto(data, MigrateLedgerRequest.parser());
            int partition = request.getPartition();
            String topic = request.getTopic();
            String original = request.getOriginal();
            String destination = request.getDestination();

            if (original.equals(destination)) {
                processFailed("Process migrate ledger failed", code, channel, answer,
                        RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, "The original and destination are same broker"));
                return;
            }

            commandExecutor.execute(() -> {
                try {
                    TopicManager topicManager = manager.getTopicManager();
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    PartitionInfo partitionInfo = topicManager.getPartitionInfo(topicPartition);
                    int ledger = partitionInfo.getLedger();
                    if (config.getServerId().equals(original)) {
                        if (!topicManager.hasLeadership(ledger)) {
                            processFailed("Process migrate ledger failed", code, channel, answer,
                                    RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("The original broker does not have leader role of %s", topicPartition)));
                            return;
                        }

                        Node destNode = manager.getClusterManager().getClusterNode(destination);
                        if (destNode == null) {
                            processFailed("Process migrate ledger failed", code, channel, answer,
                                    RemoteException.of(RemoteException.Failure.PROCESS_EXCEPTION, String.format("The destination broker %s is not in cluster", destination)));
                            return;
                        }

                        InetSocketAddress destinationAddr = new InetSocketAddress(destNode.getHost(), destNode.getPort());
                        Client innerClient = manager.getInnerClient();
                        ClientChannel clientChannel = innerClient.fetchChannel(destinationAddr);
                        Promise<MigrateLedgerResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                        promise.addListener(future -> {
                            if (future.isSuccess()) {
                                MigrateLedgerResponse response = (MigrateLedgerResponse) future.get();
                                Log log = manager.getLogManager().getLog(ledger);
                                Promise<Void> migratePromise = ImmediateEventExecutor.INSTANCE.newPromise();
                                if (answer != null) {
                                    migratePromise.addListener(f -> {
                                        if (f.isSuccess()) {
                                            answer.success(proto2Buf(channel.alloc(), response));
                                        } else {
                                            processFailed("Process migrate ledger failed", code, channel, answer, f.cause());
                                        }
                                    });
                                    log.migrate(destination, clientChannel, migratePromise);
                                } else {
                                    processFailed("Process migrate ledger failed", code, channel, answer, RemoteException.of(
                                            RemoteException.Failure.PROCESS_EXCEPTION, response.getMessage()
                                    ));
                                }
                            } else {
                                processFailed("Process migrate ledger failed", code, channel, answer, future.cause());
                            }
                        });
                        clientChannel.invoker().migrateLedger(config.getNotifyClientTimeoutMs(), promise, request);
                        recordCommand(code, bytes, System.nanoTime() - time, promise.isSuccess());
                        return;
                    }

                    if (config.getServerId().equals(destination)) {
                        topicManager.takeoverPartition(topicPartition);
                        MigrateLedgerResponse response = MigrateLedgerResponse.newBuilder().setSuccess(true).build();
                        if (answer != null) {
                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                        recordCommand(code, bytes, System.nanoTime() - time, true);
                        return;
                    }
                    processFailed("Process migrate ledger failed", code, channel, answer, RemoteException.of(
                            RemoteException.Failure.PROCESS_EXCEPTION, "The broker is neither original broker nor destination broker"
                    ));
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                } catch (Throwable t) {
                    processFailed("Process migrate ledger failed", code, channel, answer, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process migrate ledger failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processSendMessage(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            SendMessageRequest request = readProto(data, SendMessageRequest.parser());
            int ledger = request.getLedger();
            int marker = request.getMarker();
            Promise<Offset> promise = serviceExecutor.newPromise();
            promise.addListener((GenericFutureListener<Future<Offset>>) f -> {
                if (f.isSuccess()) {
                    try {
                        if (answer != null) {
                            Offset offset = f.getNow();
                            SendMessageResponse response = SendMessageResponse.newBuilder()
                                    .setLedger(ledger)
                                    .setEpoch(offset.getEpoch())
                                    .setIndex(offset.getIndex())
                                    .build();

                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                    } catch (Throwable t) {
                        processFailed("Process send message failed", code, channel, answer, t);
                    }
                } else {
                    processFailed("Process send message failed", code, channel, answer, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, f.isSuccess());
            });
            manager.getLogManager().appendRecord(ledger, marker, data, promise);
        } catch (Throwable t) {
            processFailed("Process send message failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processQueryClusterInfo(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            commandExecutor.execute(() -> {
                try {
                    String clusterName = config.getClusterName();
                    List<Node> clusterUpNodes = manager.getClusterManager().getClusterUpNodes();
                    Map<String, NodeMetadata> nodeMetadataMap = clusterUpNodes.stream().collect(
                            Collectors.toMap(Node::getId, node ->
                                    NodeMetadata.newBuilder()
                                            .setClusterName(clusterName)
                                            .setId(node.getId())
                                            .setHost(node.getHost())
                                            .setPort(node.getPort())
                                            .build()
                            )
                    );
                    ClusterInfo info = ClusterInfo.newBuilder()
                            .setCluster(
                                    ClusterMetadata.newBuilder().setName(clusterName).build()
                            )
                            .putAllNodes(nodeMetadataMap)
                            .build();
                    QueryClusterResponse response = QueryClusterResponse.newBuilder()
                            .setClusterInfo(info)
                            .build();
                    if (answer != null) {
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process query cluster info failed", code, channel, answer, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process query cluster info failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processQueryTopicInfos(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
            ProtocolStringList topicNamesList = request.getTopicNamesList();
            commandExecutor.execute(() -> {
                try {
                    TopicManager topicManager = manager.getTopicManager();
                    Set<String> topicNames = new HashSet<>();
                    if (topicNamesList.isEmpty()) {
                        topicNames.addAll(topicManager.getAllTopics());
                    } else {
                        topicNames.addAll(topicNamesList);
                    }

                    QueryTopicInfoResponse.Builder builder = QueryTopicInfoResponse.newBuilder();
                    for (String topicName : topicNames) {
                        Set<PartitionInfo> partitionInfos = topicManager.getTopicInfo(topicName);
                        if (partitionInfos == null || partitionInfos.isEmpty()) {
                            continue;
                        }

                        int topicId = partitionInfos.stream().findAny().get().getTopicId();
                        TopicMetadata topicMetadata = TopicMetadata.newBuilder()
                                .setName(topicName)
                                .setId(topicId)
                                .build();

                        TopicInfo.Builder topicInfoBuilder = TopicInfo.newBuilder().setTopic(topicMetadata);
                        for (PartitionInfo info : partitionInfos) {
                            PartitionMetadata.Builder partitionMetadataBuilder = PartitionMetadata.newBuilder()
                                    .setTopicName(topicName)
                                    .setId(info.getPartition())
                                    .setLedger(info.getLedger())
                                    .setEpoch(info.getEpoch())
                                    .setVersion(info.getVersion())
                                    .addAllReplicaNodeIds(info.getReplicas());
                            String leader = info.getLeader();
                            if (!StringUtil.isNullOrEmpty(leader)) {
                                partitionMetadataBuilder.setLeaderNodeId(leader);
                            }

                            topicInfoBuilder.putPartitions(info.getPartition(), partitionMetadataBuilder.build());
                        }

                        builder.putTopicInfos(topicName, topicInfoBuilder.build());
                    }

                    QueryTopicInfoResponse response = builder.build();
                    if (answer != null) {
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                } catch (Throwable t) {
                    processFailed("Process query topic info failed", code, channel, answer, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process query topic info failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processRestSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            ResetSubscribeRequest request = readProto(data, ResetSubscribeRequest.parser());
            IntList markers = convertMarkers(request.getMarkers());
            int ledger = request.getLedger();
            int epoch = request.getEpoch();
            long index = request.getIndex();
            Promise<Integer> promise = serviceExecutor.newPromise();
            promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                if (f.isSuccess()) {
                    if (answer != null) {
                        ResetSubscribeResponse response = ResetSubscribeResponse.newBuilder().build();
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process reset subscription failed", code, channel, answer, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogManager().resetSubscribe(ledger, epoch, index, channel, markers, promise);
        } catch (Throwable t) {
            processFailed("Process reset subscription failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processAlterSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            AlterSubscribeRequest request = readProto(data, AlterSubscribeRequest.parser());
            IntList appendMarkers = convertMarkers(request.getAppendMarkers());
            IntList deleteMarkers = convertMarkers(request.getDeleteMarkers());
            int ledger = request.getLedger();
            Promise<Integer> promise = serviceExecutor.newPromise();
            promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                if (f.isSuccess()) {
                    if (answer != null) {
                        AlterSubscribeResponse response = AlterSubscribeResponse.newBuilder().build();
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process alter subscription failed", code, channel, answer, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogManager().alterSubscribe(channel, ledger, appendMarkers, deleteMarkers, promise);
        } catch (Throwable t) {
            processFailed("Process alter subscription failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processCleanSubscription(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CleanSubscribeRequest request = readProto(data, CleanSubscribeRequest.parser());
            int ledger = request.getLedger();
            Promise<Boolean> promise = serviceExecutor.newPromise();
            promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                if (f.isSuccess()) {
                    if (answer != null) {
                        CleanSubscribeResponse response = CleanSubscribeResponse.newBuilder().build();
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process clean subscription failed", code, channel, answer, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogManager().cleanSubscribe(channel, ledger, promise);
        } catch (Throwable t) {
            processFailed("Process clean subscription failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processCreateTopic(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
            String topic = request.getTopic();
            int partition = request.getPartition();
            int replicas = request.getReplicas();
            CreateTopicConfigRequest configs = request.getConfigs();
            TopicConfig topicConfig = (configs.getSegmentRetainCount() == 0 || configs.getSegmentRetainMs() == 0 || configs.getSegmentRollingSize() == 0)
                    ? null : new TopicConfig(configs.getSegmentRollingSize(), configs.getSegmentRetainCount(), configs.getSegmentRetainMs());

            commandExecutor.execute(() -> {
                try {
                    TopicManager topicManager = manager.getTopicManager();
                    Map<String, Object> createResult = topicManager.createTopic(topic, partition, replicas, topicConfig);
                    if (answer != null) {
                        int topicId = (int) createResult.get(CorrelationIdConstants.TOPIC_ID);
                        @SuppressWarnings("unchecked")
                        Map<Integer, Set<String>> partitionReplicasMap = (Map<Integer, Set<String>>) createResult.get(CorrelationIdConstants.PARTITION_REPLICAS);
                        List<PartitionsReplicas> partitionsReplicas = partitionReplicasMap.entrySet().stream()
                                .map(
                                        entry ->
                                                PartitionsReplicas.newBuilder()
                                                        .setPartition(entry.getKey())
                                                        .addAllReplicas(entry.getValue())
                                                        .build()
                                ).toList();
                        CreateTopicResponse response = CreateTopicResponse.newBuilder()
                                .setTopic(topic)
                                .setPartitions(partition)
                                .setTopicId(topicId)
                                .addAllPartitionsReplicas(partitionsReplicas)
                                .build();
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process create topic failed", code, channel, answer, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process create topic failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void processDeleteTopic(Channel channel, int code, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            DeleteTopicRequest request = readProto(data, DeleteTopicRequest.parser());
            String topic = request.getTopic();
            commandExecutor.execute(() -> {
                try {
                    manager.getTopicManager().deleteTopic(topic);
                    if (answer != null) {
                        DeleteTopicResponse response = DeleteTopicResponse.newBuilder().build();
                        answer.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process delete topic failed", code, channel, answer, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process delete topic failed", code, channel, answer, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    private void recordCommand(int code, int bytes, long cost, boolean ret) {
        for (APIListener listener : manager.getAPIListeners()) {
            try {
                listener.onCommand(code, bytes, cost, ret);
            } catch (Throwable t) {
                logger.warn("Record process failed, listener={} code={}", listener == null ? null : listener.getClass().getSimpleName(), code, t);
            }
        }
    }

    private void processFailed(String err, int code, Channel channel, InvokeAnswer<ByteBuf> answer, Throwable throwable) {
        logger.error("{}: command={} address={}", err, code, NetworkUtils.switchAddress(channel), throwable);
        if (answer != null) {
            answer.failure(throwable);
        }
    }

    private IntList convertMarkers(ByteString markers) throws Exception {
        CodedInputStream stream = markers.newCodedInput();
        IntList markerList = new IntArrayList(markers.size() / 4);
        while (!stream.isAtEnd()) {
            markerList.add(stream.readFixed32());
        }
        return markerList;
    }
}
