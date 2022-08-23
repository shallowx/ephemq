package org.shallow.network;

import com.google.protobuf.MessageLite;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import org.shallow.RemoteException;
import org.shallow.consumer.Subscription;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.log.LogManager;
import org.shallow.log.Offset;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.Strategy;
import org.shallow.metadata.management.ClusterManager;
import org.shallow.metadata.management.TopicManager;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.elector.*;
import org.shallow.proto.server.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.stream.Collectors;

import static org.shallow.util.NetworkUtil.*;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerConfig config;
    private final BrokerManager manager;
    private final EventExecutor commandExecutor;
    private final EventExecutor quorumVoteExecutor;

    public BrokerProcessorAware(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;
        EventExecutorGroup group = newEventExecutorGroup(2, "processor-aware").next();
        this.commandExecutor = group.next();
        this.quorumVoteExecutor = group.next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case SEND_MESSAGE -> {
                    try {
                        SendMessageRequest request = readProto(data, SendMessageRequest.parser());
                        int ledger = request.getLedger();
                        String queue = request.getQueue();

                        Promise<Offset> promise = newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<Offset>>) f -> {
                            if (f.isSuccess()) {
                                Offset offset = f.get();
                                SendMessageResponse response = SendMessageResponse
                                        .newBuilder()
                                        .setLedger(ledger)
                                        .setEpoch(offset.epoch())
                                        .setIndex(offset.index())
                                        .build();

                                if (isNotNull(answer)) {
                                    answer.success(proto2Buf(channel.alloc(), response));
                                }
                            }
                        });
                        LogManager logManager = manager.getLogManager();
                        logManager.append(ledger, queue, data, promise);
                    } catch (Throwable t){
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to append message record", t);
                        }
                        answerFailed(answer,t);
                    }
                }

                case QUORUM_VOTE -> {
                    try {
                        VoteRequest request = readProto(data, VoteRequest.parser());
                        quorumVoteExecutor.execute(() -> {
                            try {
                                int term = request.getTerm();
                                Promise<VoteResponse> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<VoteResponse>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });
                                SRaftProcessController controller = manager.getController();
                                controller.respondVote(term, promise);
                            } catch (Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to quorum vote with address<{}>, cause:{}", channel.remoteAddress().toString(), e);
                                }
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to quorum vote with address<{}>, cause:{}", channel.remoteAddress().toString(), e);
                        }
                        answerFailed(answer, e);
                    }
                }

                case HEARTBEAT -> {
                    try {
                        RaftHeartbeatRequest request = readProto(data, RaftHeartbeatRequest.parser());
                        quorumVoteExecutor.execute(() -> {
                            try {
                                int term = request.getTerm();
                                int distributedValue = request.getDistributedValue();
                                SRaftProcessController controller = manager.getController();
                                SocketAddress leader = channel.remoteAddress();
                                controller.receiveHeartbeat(term, distributedValue, leader);
                            } catch (Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to send heartbeat with address<{}>, cause:{}", channel.remoteAddress().toString(), e);
                                }
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }

                case SUBSCRIBE -> {
                    try {
                        SubscribeRequest request = readProto(data, SubscribeRequest.parser());
                        commandExecutor.execute(() -> {
                            try {
                                int ledger = request.getLedger();
                                int epoch = request.getEpoch();
                                long index = request.getIndex();
                                String queue = request.getQueue();

                                Promise<Subscription> promise = newImmediatePromise();
                                promise.addListener(future -> {
                                    if (future.isSuccess()) {
                                        Subscription subscription = (Subscription)future.get();
                                        SubscribeResponse response = SubscribeResponse
                                                .newBuilder()
                                                .setEpoch(subscription.getEpoch())
                                                .setLedger(ledger)
                                                .setIndex(subscription.getIndex())
                                                .setQueue(queue)
                                                .build();

                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), response));
                                        } else {
                                            answerFailed(answer, future.cause());
                                        }
                                    }
                                });

                                LogManager logManager = manager.getLogManager();
                                logManager.subscribe(queue, ledger, epoch, index, promise);
                            } catch (Throwable t) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to subscribe");
                                }
                                answerFailed(answer, t);
                            }
                        });
                    } catch (Throwable t) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to subscribe");
                        }
                        answerFailed(answer, t);
                    }
                }

                case CREATE_TOPIC -> {
                    try {
                        CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
                        commandExecutor.execute(() -> {
                            try {
                                String topic = request.getTopic();
                                int partitions = request.getPartitions();
                                int latencies = request.getLatencies();

                                if (logger.isDebugEnabled()) {
                                    logger.debug("The topic<{}> partitions<{}> latency<{}>", topic, partitions, latencies);
                                }

                                Promise<MessageLite> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            CreateTopicPrepareCommitResponse prepareCommitResponse = (CreateTopicPrepareCommitResponse) f.get();

                                            CreateTopicResponse response = CreateTopicResponse
                                                    .newBuilder()
                                                    .setLatencies(prepareCommitResponse.getLatencies())
                                                    .setAck(1)
                                                    .setPartitions(prepareCommitResponse.getPartitions())
                                                    .setTopic(prepareCommitResponse.getTopic())
                                                    .build();
                                            answer.success(proto2Buf(channel.alloc(), response));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });

                                SRaftProcessController controller = manager.getController();
                                TopicRecord topicRecord = new TopicRecord(topic, partitions, latencies);
                                CommitRecord<TopicRecord> record = new CommitRecord<>(topicRecord, CommitType.ADD);
                                controller.prepareCommit(Strategy.TOPIC, record, promise);
                            } catch (Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to create topic with address<{}>, cause:{}", channel.remoteAddress().toString(), e);
                                }
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }

                case DELETE_TOPIC -> {
                    try {
                        DelTopicRequest request = readProto(data, DelTopicRequest.parser());
                        String topic = request.getTopic();

                        commandExecutor.execute(() -> {
                            try {
                                Promise<MessageLite> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            DeleteTopicPrepareCommitResponse prepareCommitResponse = (DeleteTopicPrepareCommitResponse) f.get();

                                            DelTopicResponse response = DelTopicResponse
                                                    .newBuilder()
                                                    .setTopic(prepareCommitResponse.getTopic())
                                                    .setAck(1)
                                                    .setTopic(prepareCommitResponse.getTopic())
                                                    .build();

                                            answer.success(proto2Buf(channel.alloc(), response));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });

                                SRaftProcessController controller = manager.getController();
                                TopicRecord topicRecord = new TopicRecord(topic);
                                CommitRecord<TopicRecord> record = new CommitRecord<>(topicRecord, CommitType.REMOVE);
                                controller.prepareCommit(Strategy.TOPIC, record, promise);
                            } catch (Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to delete topic<{}> with address<{}>, cause:{}", topic, channel.remoteAddress().toString(), e);
                                }
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to delete topic with address<{}>, cause:{}", channel.remoteAddress().toString(), e);
                        }
                        answerFailed(answer, e);
                    }
                }

                case PREPARE_COMMIT -> {
                    try {
                        commandExecutor.execute(() ->{});
                    } catch (Throwable t){
                        answerFailed(answer,t);
                    }
                }

                case POST_COMMIT -> {
                    try {
                        commandExecutor.execute(() ->{});
                    } catch (Throwable t){
                        answerFailed(answer,t);
                    }
                }

                case FETCH_CLUSTER_RECORD -> {
                    try {
                        QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
                        commandExecutor.execute(() -> {
                            try {
                                String cluster = request.getCluster();

                                ClusterManager clusterManager = manager.getController().getClusterManager();
                                Set<NodeRecord> clusterInfo = clusterManager.getClusterInfo(cluster);

                                QueryClusterNodeResponse.Builder response = QueryClusterNodeResponse.newBuilder();
                                if (!clusterInfo.isEmpty()) {
                                    Set<NodeMetadata> nodeMetadataSets = clusterInfo.stream().map(nodeRecord -> {
                                        InetSocketAddress socketAddress = (InetSocketAddress)nodeRecord.getSocketAddress();
                                        String host = socketAddress.getHostName();
                                        int port = socketAddress.getPort();

                                        return NodeMetadata
                                                .newBuilder()
                                                .setCluster(config.getClusterName())
                                                .setName(nodeRecord.getName())
                                                .setPort(port)
                                                .setHost(host)
                                                .build();
                                    }).collect(Collectors.toSet());

                                    response.addAllNodes(nodeMetadataSets);
                                }

                                if (isNotNull(answer)) {
                                    answer.success(proto2Buf(channel.alloc(), response.build()));
                                }
                            } catch (Throwable t) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to query cluster information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                                }
                                answerFailed(answer,t);
                            }
                        });
                    } catch (Throwable t){
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to query cluster information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                        }
                        answerFailed(answer,t);
                    }
                }

                case FETCH_TOPIC_RECORD -> {
                    try {
                        QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
                        commandExecutor.execute(() -> {
                            try {
                                ProtocolStringList topics = request.getTopicList();
                                TopicManager topicManager = manager.getController().getTopicManager();
                                if (topics.isEmpty()) {
                                    topics.addAll(topicManager.getAllTopics());
                                }


                                QueryTopicInfoResponse.Builder response = QueryTopicInfoResponse.newBuilder();

                                for (String topic : topics) {
                                    Set<PartitionRecord> partitionRecords = topicManager.getTopicInfo(topic);

                                    if (!partitionRecords.isEmpty()) {
                                        Iterator<PartitionRecord> iterator = partitionRecords.iterator();
                                        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();
                                        while (iterator.hasNext()) {
                                            PartitionRecord partitionRecord = iterator.next();
                                            partitionMetadataMap.put(partitionRecord.getId(), PartitionMetadata.newBuilder()
                                                    .setId(partitionRecord.getId())
                                                    .setLeader(partitionRecord.getLeader())
                                                    .setLatency(partitionRecord.getLatency())
                                                    .addAllReplicas(partitionRecord.getLatencies())
                                                    .build());
                                        }

                                        TopicMetadata metadata = TopicMetadata.newBuilder()
                                                .setName(topic)
                                                .putAllPartitions(partitionMetadataMap)
                                                .build();

                                        Map<String, TopicMetadata> topicMetadataMap = new HashMap<>();
                                        topicMetadataMap.put(topic, metadata);
                                        response.putAllTopics(topicMetadataMap).build();
                                    }
                                }

                                if (isNotNull(answer)) {
                                    answer.success(proto2Buf(channel.alloc(), response.build()));
                                }
                            } catch (Throwable t) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to query topic information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                                }
                                answerFailed(answer, t);
                            }
                        });
                    } catch (Throwable t){
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to query topic information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                        }
                        answerFailed(answer,t);
                    }
                }

                case REGISTER_NODE -> {
                    try {
                        commandExecutor.execute(() -> {
                            try {
                                RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                                String cluster = request.getCluster();

                                NodeMetadata metadata = request.getMetadata();
                                String name = metadata.getName();
                                String host = metadata.getHost();
                                int port = metadata.getPort();

                                NodeRecord nodeRecord = new NodeRecord(cluster, name, switchSocketAddress(host, port));
                                CommitRecord<NodeRecord> commitRecord = new CommitRecord<>(nodeRecord, CommitType.ADD);

                                Promise<MessageLite> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });

                                SRaftProcessController controller = manager.getController();
                                controller.prepareCommit(Strategy.CLUSTER, commitRecord, promise);
                            } catch (Throwable t) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to register node, cause:{}", t);
                                }
                                answerFailed(answer,t);
                            }
                        });
                    } catch (Throwable t){
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to register node, cause:{}", t);
                        }
                        answerFailed(answer,t);
                    }
                }

                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel<{}> - command [{}]", switchAddress(channel), command);
            }
            answerFailed(answer, cause);
        }
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (isNotNull(answer)) {
            answer.failure(cause);
        }
    }
}
