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
import org.shallow.metadata.sraft.LeaderElector;
import org.shallow.metadata.sraft.RaftVoteProcessor;
import org.shallow.metadata.snapshot.ClusterSnapshot;
import org.shallow.metadata.snapshot.TopicSnapshot;
import org.shallow.processor.Ack;
import org.shallow.processor.AwareInvocation;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.elector.*;
import org.shallow.proto.server.*;
import org.shallow.util.NetworkUtil;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import static org.shallow.util.NetworkUtil.*;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

@SuppressWarnings("all")
public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerProcessorAware.class);

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

        manager.getBrokerConnectionManager().add(channel);

        channel.closeFuture().addListener(f -> {
            LedgerManager ledgerManager = manager.getLedgerManager();
            ledgerManager.clearChannel(channel);
        });
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

                case CLEAN_SUBSCRIBE -> processCleanSubscribeRequest(channel, command, data, answer, type, version);

                case CREATE_TOPIC -> processCreateTopicRequest(channel, command, data, answer, type, version);

                case DELETE_TOPIC -> processDelTopicRequest(channel, command, data, answer, type, version);

                case FETCH_CLUSTER_RECORD -> processFetchClusterRecordRequest(channel, command, data, answer, type, version);

                case FETCH_TOPIC_RECORD -> processFetchTopicRecordRequest(channel, command, data, answer, type, version);

                case REGISTER_NODE -> processRegisterNodeRequest(channel, command, data, answer, type, version);

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
                    LedgerManager logManager = manager.getLedgerManager();
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
                    String topic = request.getTopic();

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

                    LedgerManager logManager = manager.getLedgerManager();
                    logManager.pull(channel, ledger, topic, queue, theVersion, epoch, index, limit, promise);
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
                    int version = request.getVersion();
                    int term = request.getTerm();

                    Promise<Boolean> promise = NetworkUtil.newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<Boolean>>) future -> {
                        boolean callback = future.get();
                        VoteResponse response = VoteResponse
                                .newBuilder()
                                .setAck(callback)
                                .build();

                        if (answer != null) {
                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                    });

                    RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                    voteProcessor.handleVoteRequest(term, version, promise);
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
                    int theVersion = request.getVersion();
                    String leader = request.getLeader();

                    Promise<RaftHeartbeatResponse> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<RaftHeartbeatResponse>>) future -> {
                        RaftHeartbeatResponse response = RaftHeartbeatResponse
                                .newBuilder()
                                .build();

                        if (answer != null) {
                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                    });

                    RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                    SocketAddress socketAddress = channel.remoteAddress();
                    voteProcessor.handleHeartbeatRequest(socketAddress, leader, theVersion, term, distributedValue);
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
                    String topic = request.getTopic();

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
                                    .setVersion(subscription.version())
                                    .build();

                            if (answer != null) {
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        }else {
                            answerFailed(answer, future.cause());
                        }
                    });

                    LedgerManager logManager = manager.getLedgerManager();
                    logManager.subscribe(channel, topic, queue, theVersion, ledger, epoch, index, promise);
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

    private void processCreateTopicRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
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

                        Promise<Void> promise = newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<Void>>) f -> {
                            if (f.isSuccess()) {
                                if (answer != null) {
                                    CreateTopicResponse response = CreateTopicResponse
                                            .newBuilder()
                                            .setTopic(topic)
                                            .setPartitions(partitions)
                                            .setLatencies(latencies)
                                            .setAck(Ack.SUCCESS)
                                            .build();
                                    answer.success(proto2Buf(channel.alloc(), response));
                                }
                            } else {
                                answerFailed(answer, f.cause());
                            }
                        });

                        RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                        TopicSnapshot topicSnapshot = voteProcessor.getTopicSnapshot();
                        topicSnapshot.create(topic, partitions, latencies);

                        promise.trySuccess(null);
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
        });
    }

    private void processDelTopicRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
            try {
                DelTopicRequest request = readProto(data, DelTopicRequest.parser());
                String topic = request.getTopic();

                commandExecutor.execute(() -> {
                    try {
                        Promise<MessageLite> promise = newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<MessageLite>>) f -> {
                            if (f.isSuccess()) {
                                if (answer != null) {
                                    DelTopicResponse response = DelTopicResponse.newBuilder().build();
                                    answer.success(proto2Buf(channel.alloc(), response));
                                }
                            } else {
                                answerFailed(answer, f.cause());
                            }
                        });

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
        });
    }

    private void processRegisterNodeRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
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

                        Promise<Void> promise = NetworkUtil.newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<Void>>) future -> {
                            if (future.isSuccess()) {
                                RegisterNodeResponse response = RegisterNodeResponse
                                        .newBuilder()
                                        .setServerId(name)
                                        .setPort(port)
                                        .setState(state)
                                        .setHost(host)
                                        .build();

                                if (answer != null) {
                                    answer.success(proto2Buf(channel.alloc(), response));
                                }
                            } else {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to register node, cause:{}", future.cause());
                                }
                                answerFailed(answer,future.cause());
                            }
                        });

                        RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                        ClusterSnapshot clusterSnapshot = voteProcessor.getClusterSnapshot();
                        clusterSnapshot.registerNode(cluster, name, host, port, state, promise);
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
        });
    }

    private void processFetchTopicRecordRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
            try {
                QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
                commandExecutor.execute(() -> {
                    try {
                        ProtocolStringList topics = request.getTopicList();
                        RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                        TopicSnapshot topicSnapshot = voteProcessor.getTopicSnapshot();

                        QueryTopicInfoResponse.Builder builder = QueryTopicInfoResponse.newBuilder();
                        for (String topic : topics) {
                            TopicRecord record = topicSnapshot.getRecord(topic);

                            if (record == null) {
                                continue;
                            }

                            Set<PartitionRecord> partitionRecords = record.getPartitionRecords();
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
                                builder.putAllTopics(topicMetadataMap);
                            }
                        }

                        if (answer != null) {
                            answer.success(proto2Buf(channel.alloc(), builder.build()));
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
        });
    }

    private void processFetchClusterRecordRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
            try {
                QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
                commandExecutor.execute(() -> {
                    try {
                        String cluster = request.getCluster();
                        RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                        ClusterSnapshot clusterSnapshot = voteProcessor.getClusterSnapshot();
                        Set<NodeRecord> nodeRecords = clusterSnapshot.getNodeRecord(cluster);

                        QueryClusterNodeResponse.Builder builder = QueryClusterNodeResponse.newBuilder();
                        if (answer != null) {
                            if (nodeRecords.isEmpty()) {
                                answer.success(proto2Buf(channel.alloc(), builder.build()));
                                return;
                            }

                            for (NodeRecord record : nodeRecords) {
                                InetSocketAddress socketAddress = (InetSocketAddress) record.getSocketAddress();

                                String host = socketAddress.getHostName();
                                int port = socketAddress.getPort();

                                NodeMetadata metadata = NodeMetadata
                                        .newBuilder()
                                        .setCluster(record.getCluster())
                                        .setPort(port)
                                        .setHost(host)
                                        .setState(record.getState())
                                        .setName(record.getName())
                                        .build();

                                builder.addNodes(metadata);
                            }

                            answer.success(proto2Buf(channel.alloc(), builder.build()));
                        }
                    } catch (Throwable t) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to query cluster info");
                            answerFailed(answer, t);
                        }
                    }
                });
            } catch (Throwable t){
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to query cluster information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                }
                answerFailed(answer,t);
            }
        });
    }

    private void transfer(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version, TransferFunction function) {
        try {
            if (config.isStandAlone()) {
                function.get();
            } else {
                RaftVoteProcessor voteProcessor = manager.getVoteProcessor();
                LeaderElector leaderElector = voteProcessor.getLeaderElector();
                String leader = leaderElector.getLeader();

                Promise<Void> promise = newImmediatePromise();
                promise.addListener(future -> {
                    if (!future.isSuccess()) {
                        answerFailed(answer, future.cause());
                    }
                });

                boolean callback = true;

                if (leader == null || leader.isEmpty()) {
                    callback = false;
                    promise.tryFailure(new RuntimeException("The quorum leader not found"));
                }

                if (!leaderElector.isLeader()) {
                    callback = false;
                    AwareInvocation invocation = AwareInvocation.newInvocation(command, version, data, type, 5000, answer);
                    channel.writeAndFlush(invocation);
                }

                if (callback) {
                    function.get();
                }
            }
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processCleanSubscribeRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
        transfer(channel, command, data, answer, type, version, () -> {
            try {
                CleanSubscribeRequest request = readProto(data, CleanSubscribeRequest.parser());
                commandExecutor.execute(() -> {
                    try {
                        String queue = request.getQueue();
                        String topic = request.getTopic();
                        int ledgerId = request.getLedgerId();

                        Promise<Void> promise = newImmediatePromise();
                        promise.addListener(future -> {
                            if (future.isSuccess()) {
                                CleanSubscribeResponse response = CleanSubscribeResponse.newBuilder().build();
                                if (answer != null) {
                                    answer.success(proto2Buf(channel.alloc(), response));
                                } else {
                                    answerFailed(answer, future.cause());
                                }
                            }
                        });

                        LedgerManager ledgerManager = manager.getLedgerManager();
                        ledgerManager.clean(channel, topic, queue, ledgerId, promise);
                    } catch (Throwable t) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to query cluster info");
                            answerFailed(answer, t);
                        }
                    }
                });
            } catch (Throwable t){
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to query cluster information with address<{}>, cause:{}", channel.remoteAddress().toString(), t);
                }
                answerFailed(answer,t);
            }
        });
    }

    @FunctionalInterface
    interface TransferFunction {
        void get();
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
