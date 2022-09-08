package org.shallow.network;

import com.google.protobuf.MessageLite;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import org.shallow.RemoteException;
import org.shallow.consumer.push.Subscription;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.log.LedgerManager;
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
import org.shallow.processor.Ack;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.elector.*;
import org.shallow.proto.server.*;
import org.shallow.util.ByteBufUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.stream.Collectors;

import static org.shallow.util.NetworkUtil.*;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerConfig config;
    private final BrokerManager manager;
    private final EventExecutor commandExecutor;
    private final EventExecutor quorumVoteExecutor;
    private final EventExecutor handleMessageExecutor;

    public BrokerProcessorAware(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;

        EventExecutorGroup group = newEventExecutorGroup(config.getProcessCommandHandleThreadLimit(), "processor-command").next();
        this.commandExecutor = group.next();
        this.quorumVoteExecutor = group.next();

        this.handleMessageExecutor = newEventExecutorGroup(Runtime.getRuntime().availableProcessors(), "processor-handle-message").next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        try {
            switch (command) {

                case SEND_MESSAGE -> processSendRequest(channel, data, answer, version);

                case PULL_MESSAGE -> processPullRequest(channel, data, answer);

                case QUORUM_VOTE -> processQuorumVoteRequest(channel, data, answer);

                case HEARTBEAT -> processHeartbeatRequest(channel, data, answer);

                case SUBSCRIBE -> processSubscribeRequest(channel, data, answer);

                case CREATE_TOPIC -> processCreateTopicRequest(channel, data, answer);

                case DELETE_TOPIC -> processDelTopicRequest(channel, data, answer);

                case FETCH_CLUSTER_RECORD -> processFetchClusterRecordRequest(channel, data, answer);

                case FETCH_TOPIC_RECORD -> processFetchTopicRecordRequest(channel, data, answer);

                case REGISTER_NODE -> processRegisterNodeRequest(channel, data, answer);

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

    private void processSendRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer, short version) {
        try {
            SendMessageRequest request = readProto(data, SendMessageRequest.parser());

            ByteBuf retain = data.retain();
            handleMessageExecutor.execute(() -> {
                try {
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

                            if (answer != null) {
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            answerFailed(answer, f.cause());
                        }
                    });
                    LedgerManager logManager = manager.getLogManager();
                    logManager.append(ledger, queue, retain, version, promise);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to append message record", t);
                    }
                    answerFailed(answer,t);
                }
            });
        } catch (Throwable t){
            if (logger.isErrorEnabled()) {
                logger.error("Failed to append message record", t);
            }
            answerFailed(answer,t);
        }
    }

    private void processPullRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer)  {
        try {
            PullMessageRequest request = readProto(data, PullMessageRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    int ledger = request.getLedger();
                    int limit = request.getLimit();
                    String queue = request.getQueue();
                    short theVersion = (short) request.getVersion();
                    int epoch = request.getEpoch() ;
                    long index = request.getIndex();

                    Promise<PullMessageResponse> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<PullMessageResponse>>) future -> {
                        if (future.isSuccess()) {
                            if (answer != null) {
                                answer.success(proto2Buf(channel.alloc(), future.get()));
                            }
                        } else {
                            answerFailed(answer, future.cause());
                        }
                    });

                    LedgerManager logManager = manager.getLogManager();
                    logManager.pull(channel, ledger, queue, theVersion, epoch, index, limit, promise);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to pull message record", t);
                    }
                    answerFailed(answer,t);
                }
            });
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to pull message record", t);
            }
            answerFailed(answer,t);
        }
    }

    private void processQuorumVoteRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer)  {
        try {
            VoteRequest request = readProto(data, VoteRequest.parser());
            quorumVoteExecutor.execute(() -> {
                try {
                    int term = request.getTerm();
                    Promise<VoteResponse> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<VoteResponse>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
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

    private void processHeartbeatRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
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

    private void processSubscribeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            SubscribeRequest request = readProto(data, SubscribeRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    int ledger = request.getLedger();
                    int epoch = request.getEpoch();
                    long index = request.getIndex();
                    String queue = request.getQueue();
                    short theVersion = (short) request.getVersion();

                    Promise<Subscription> promise = newImmediatePromise();
                    promise.addListener(future -> {
                        if (future.isSuccess()) {
                            Subscription subscription = (Subscription)future.get();
                            SubscribeResponse response = SubscribeResponse
                                    .newBuilder()
                                    .setEpoch(subscription.epoch())
                                    .setLedger(ledger)
                                    .setIndex(subscription.index())
                                    .setQueue(queue)
                                    .build();

                            if (answer != null) {
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        }else {
                            answerFailed(answer, future.cause());
                        }
                    });

                    LedgerManager logManager = manager.getLogManager();
                    logManager.subscribe(channel, queue, theVersion, ledger, epoch, index, promise);
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

    private void processCreateTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
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
                            if (answer != null) {
                                CreateTopicPrepareCommitResponse prepareCommitResponse = (CreateTopicPrepareCommitResponse) f.get();

                                CreateTopicResponse response = CreateTopicResponse
                                        .newBuilder()
                                        .setLatencies(prepareCommitResponse.getLatencies())
                                        .setAck(Ack.SUCCESS)
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

    private void processDelTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            DelTopicRequest request = readProto(data, DelTopicRequest.parser());
            String topic = request.getTopic();

            commandExecutor.execute(() -> {
                try {
                    Promise<MessageLite> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
                                DeleteTopicPrepareCommitResponse prepareCommitResponse = (DeleteTopicPrepareCommitResponse) f.get();

                                DelTopicResponse response = DelTopicResponse
                                        .newBuilder()
                                        .setTopic(prepareCommitResponse.getTopic())
                                        .setAck(Ack.SUCCESS)
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

    private void processRegisterNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            commandExecutor.execute(() -> {
                try {
                    RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                    String cluster = request.getCluster();

                    NodeMetadata metadata = request.getMetadata();
                    String name = metadata.getName();
                    String host = metadata.getHost();
                    String state = metadata.getState();
                    int port = metadata.getPort();

                    NodeRecord nodeRecord = new NodeRecord(cluster, name, state, switchSocketAddress(host, port));
                    CommitRecord<NodeRecord> commitRecord = new CommitRecord<>(nodeRecord, CommitType.ADD);

                    Promise<MessageLite> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
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

    private void processFetchTopicRecordRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
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

                    if (answer != null) {
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

    private void processFetchClusterRecordRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
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

                    if (answer != null) {
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

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
