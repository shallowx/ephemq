package org.ephemq.remoting;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import io.netty.util.internal.StringUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.ephemq.client.core.Client;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Node;
import org.ephemq.common.message.PartitionInfo;
import org.ephemq.common.message.TopicConfig;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.config.CommonConfig;
import org.ephemq.config.NetworkConfig;
import org.ephemq.remote.proto.*;
import org.ephemq.remote.proto.server.*;
import org.ephemq.zookeeper.CorrelationIdConstants;
import org.ephemq.ledger.Log;
import org.ephemq.listener.APIListener;
import org.ephemq.remote.exception.RemotingException;
import org.ephemq.remote.invoke.Command;
import org.ephemq.remote.invoke.InvokedFeedback;
import org.ephemq.remote.invoke.Processor;
import org.ephemq.remote.util.NetworkUtil;
import org.ephemq.remote.util.ProtoBufUtil;
import org.ephemq.support.Manager;
import org.ephemq.zookeeper.TopicHandleSupport;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.ephemq.remote.util.ProtoBufUtil.proto2Buf;
import static org.ephemq.remote.util.ProtoBufUtil.readProto;

/**
 * ServiceProcessor is responsible for handling various service-related commands
 * and processing them accordingly. It manages channel activities and command
 * executions using an internal manager and executor.
 */
public class ServiceProcessor implements Processor, Command.Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServiceProcessor.class);
    /**
     * Holds the common configuration settings required by the ServiceProcessor.
     * This variable is initialized during the construction of a ServiceProcessor
     * instance and is used internally by various processing methods to ensure
     * operations conform to the given configuration parameters.
     */
    protected final CommonConfig commonConfiguration;
    /**
     * The manager responsible for handling various operations within the service
     * processor.
     * <p>
     * It manages the lifecycle of service components, such as starting
     * and shutting down services, handling connections, managing topics,
     * cluster operations, and logging. It also supports events and metrics
     * listeners management, and provides executor groups for different
     * types of event handling.
     */
    protected final Manager manager;
    /**
     * Manages the execution of commands for the service processor.
     * This executor handles tasks related to command processing in a concurrent environment.
     */
    protected final EventExecutor commandExecutor;
    /**
     * Holds the network configuration settings for the service processor.
     * This configuration is used to manage various aspects such as connection timeouts,
     * buffer sizes, and thread limits for network operations.
     */
    private final NetworkConfig networkConfiguration;
    /**
     *
     */
    protected EventExecutor executor;

    /**
     * Constructs a ServiceProcessor instance using the provided configurations and manager.
     *
     * @param commonConfiguration the common configuration settings to be used by this service processor
     * @param networkConfiguration the network configuration settings to be used by this service processor
     * @param manager the manager responsible for handling command execution events
     */
    public ServiceProcessor(CommonConfig commonConfiguration, NetworkConfig networkConfiguration, Manager manager) {
        this.manager = manager;
        this.commonConfiguration = commonConfiguration;
        this.networkConfiguration = networkConfiguration;
        this.commandExecutor = manager.getCommandHandleEventExecutorGroup().next();
    }

    /**
     * Invoked when a channel becomes active. This method sets the executor,
     * adds the channel to the connection manager, and sets up listeners for
     * the channel close event to handle log subscriptions.
     *
     * @param channel  the active channel
     * @param executor the executor to be used for asynchronous event handling
     */
    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        this.executor = executor;
        this.manager.getConnection().add(channel);
        channel.closeFuture().addListener(future -> {
            for (Log log : manager.getLogHandler().getLedgerIdOfLogs().values()) {
                log.cleanSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
                log.subscribeSynchronize(channel, ImmediateEventExecutor.INSTANCE.newPromise());
            }
        });
    }

    /**
     * Processes the given command received from a network channel.
     *
     * @param channel  the network channel through which the command was received
     * @param code     the code identifying the command type
     * @param data     the data buffer containing the command payload
     * @param feedback a callback interface for providing a response or handling errors
     */
    @Override
    public void process(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        int length = data.readableBytes();
        try {
            switch (code) {
                case SEND_MESSAGE -> processSendMessage(channel, code, data, feedback);
                case QUERY_CLUSTER_INFOS -> processQueryClusterInfo(channel, code, data, feedback);
                case QUERY_TOPIC_INFOS -> processQueryTopicInfos(channel, code, data, feedback);
                case REST_SUBSCRIBE -> processRestSubscription(channel, code, data, feedback);
                case ALTER_SUBSCRIBE -> processAlterSubscription(channel, code, data, feedback);
                case CLEAN_SUBSCRIBE -> processCleanSubscription(channel, code, data, feedback);
                case CREATE_TOPIC -> processCreateTopic(channel, code, data, feedback);
                case DELETE_TOPIC -> processDeleteTopic(channel, code, data, feedback);
                case MIGRATE_LEDGER -> processMigrateLedger(channel, code, data, feedback);
                case SYNC_LEDGER -> processSyncLedger(channel, code, data, feedback);
                case CANCEL_SYNC_LEDGER -> processUnSyncLedger(channel, code, data, feedback);
                case CALCULATE_PARTITIONS -> processCalculatePartitions(channel, code, data, feedback);
                default -> {
                    if (feedback != null) {
                        feedback.failure(RemotingException.of(RemotingException.Failure.UNSUPPORTED_EXCEPTION,
                                String.format("Unsupported command[code=%s] and message length is %s", code, length)));
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel[{}] command[{}] unsupported, length={}", code, NetworkUtil.switchAddress(channel), length);
                    }
                }
            }
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug(t.getMessage(), t);
            }

            if (feedback != null) {
                feedback.failure(t);
            }
        }
    }

    /**
     * Processes synchronization ledger requests and handles the response feedback.
     *
     * @param channel the Netty channel through which the request was received
     * @param code the operation code associated with the request
     * @param data the ByteBuf containing the serialized request data
     * @param feedback the InvokedFeedback instance to handle success or failure responses
     */
    protected void processSyncLedger(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            SyncRequest request = readProto(data, SyncRequest.parser());
            int ledger = request.getLedger();
            int epoch = request.getEpoch();
            long index = request.getIndex();

            Promise<SyncResponse> promise = executor.newPromise();
            promise.addListener((GenericFutureListener<Future<SyncResponse>>) f -> {
                if (f.isSuccess()) {
                    try {
                        if (feedback != null) {
                            SyncResponse response = f.getNow();
                            feedback.success(proto2Buf(channel.alloc(), response));
                        }
                    } catch (Exception e) {
                        processFailed(String.format("Process sync ledger[%s] is failure", ledger), code, channel, feedback, e);
                    }
                } else {
                    processFailed(String.format("Process sync ledger[%s] is failure", ledger), code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, f.isSuccess());
            });
            manager.getTopicHandleSupport().getParticipantSuooprt()
                    .subscribeLedger(ledger, epoch, index, channel, promise);
        } catch (Exception e) {
            processFailed("Process sync ledger failed", code, channel, feedback, e);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }

    }

    /**
     * Processes the request to unsync a ledger.
     *
     * @param channel The communication channel.
     * @param code The operation code indicating the type of request.
     * @param data The data associated with the request.
     * @param feedback The feedback mechanism to communicate the result of the operation.
     */
    protected void processUnSyncLedger(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CancelSyncRequest request = readProto(data, CancelSyncRequest.parser());
            int ledger = request.getLedger();
            Promise<Void> promise = executor.newPromise();
            promise.addListener(f -> {
                if (f.isSuccess()) {
                    try {
                        if (feedback != null) {
                            CancelSyncResponse response = CancelSyncResponse.newBuilder().build();
                            feedback.success(proto2Buf(channel.alloc(), response));
                        }
                    } catch (Exception e) {
                        processFailed(String.format("Process un-sync ledger[%s] is failure", ledger), code, channel, feedback, e);
                    }
                } else {
                    processFailed(String.format("Process un-sync ledger[%s] is failure", ledger), code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, f.isSuccess());
            });

            manager.getTopicHandleSupport().getParticipantSuooprt().unSubscribeLedger(ledger, channel, promise);
        } catch (Exception e) {
            processFailed("Process un-sync ledger failed", code, channel, feedback, e);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the calculation of partitions and sends the result back through the provided feedback channel.
     *
     * @param channel The channel through which the request is being executed.
     * @param code An integer code representing the command.
     * @param data The input data buffer containing necessary information for processing.
     * @param feedback The feedback mechanism to report success or failure of processing.
     */
    protected void processCalculatePartitions(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            commandExecutor.execute(() -> {
                try {
                    TopicHandleSupport support = manager.getTopicHandleSupport();
                    Map<String, Integer> partitions = support.calculatePartitions();
                    CalculatePartitionsResponse.Builder response = CalculatePartitionsResponse.newBuilder();
                    if (partitions != null) {
                        response.putAllPartitions(partitions);
                    }

                    if (feedback != null) {
                        feedback.success(ProtoBufUtil.proto2Buf(channel.alloc(), response.build()));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Exception e) {
                    processFailed("Process calculate partition failed", code, channel, feedback, e);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Exception e) {
            processFailed("Process calculate partition failed", code, channel, feedback, e);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the migration of a ledger from an original broker to a destination broker.
     *
     * @param channel the channel through which the migration request is received
     * @param code a unique code representing the type of request
     * @param data the byte buffer containing the data of the migration request
     * @param feedback feedback callback to send the result of the migration process
     */
    protected void processMigrateLedger(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            MigrateLedgerRequest request = readProto(data, MigrateLedgerRequest.parser());
            int partition = request.getPartition();
            String topic = request.getTopic();
            String original = request.getOriginal();
            String destination = request.getDestination();

            if (original.equals(destination)) {
                processFailed("Process migrate ledger failed", code, channel, feedback,
                        RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                                "The original and destination are the same broker"));
                return;
            }

            commandExecutor.execute(() -> {
                try {
                    TopicHandleSupport support = manager.getTopicHandleSupport();
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    PartitionInfo partitionInfo = support.getPartitionInfo(topicPartition);
                    int ledger = partitionInfo.getLedger();
                    if (commonConfiguration.getServerId().equals(original)) {
                        if (!support.hasLeadership(ledger)) {
                            processFailed("Process migrate ledger failed", code, channel, feedback,
                                    RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                                            String.format("The original broker does not have a leader role of %s",
                                                    topicPartition)));
                            return;
                        }

                        Node destNode = manager.getClusterManager().getClusterReadyNode(destination);
                        if (destNode == null) {
                            processFailed("Process migrate ledger failed", code, channel, feedback,
                                    RemotingException.of(RemotingException.Failure.PROCESS_EXCEPTION,
                                            String.format("The destination broker %s is not in cluster", destination)));
                            return;
                        }

                        InetSocketAddress destinationAddr = new InetSocketAddress(destNode.getHost(), destNode.getPort());
                        Client innerClient = manager.getInternalClient();
                        ClientChannel clientChannel = innerClient.getActiveChannel(destinationAddr);
                        Promise<MigrateLedgerResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
                        promise.addListener(future -> {
                            if (future.isSuccess()) {
                                MigrateLedgerResponse response = (MigrateLedgerResponse) future.get();
                                Log log = manager.getLogHandler().getLog(ledger);
                                Promise<Void> migratePromise = ImmediateEventExecutor.INSTANCE.newPromise();
                                if (feedback != null) {
                                    migratePromise.addListener(f -> {
                                        if (f.isSuccess()) {
                                            feedback.success(proto2Buf(channel.alloc(), response));
                                        } else {
                                            processFailed("Process migrate ledger failed", code, channel, feedback, f.cause());
                                        }
                                    });
                                    log.migrate(destination, clientChannel, migratePromise);
                                } else {
                                    processFailed("Process migrate ledger failed", code, channel, feedback,
                                            RemotingException.of(
                                                    RemotingException.Failure.PROCESS_EXCEPTION, response.getMessage()
                                    ));
                                }
                            } else {
                                processFailed("Process migrate ledger failed", code, channel, feedback, future.cause());
                            }
                        });
                        clientChannel.invoker().migrateLedger(networkConfiguration.getNotifyClientTimeoutMilliseconds(), promise, request);
                        recordCommand(code, bytes, System.nanoTime() - time, promise.isSuccess());
                        return;
                    }

                    if (commonConfiguration.getServerId().equals(destination)) {
                        support.takeoverPartition(topicPartition);
                        MigrateLedgerResponse response = MigrateLedgerResponse.newBuilder().setSuccess(true).build();
                        if (feedback != null) {
                            feedback.success(proto2Buf(channel.alloc(), response));
                        }
                        recordCommand(code, bytes, System.nanoTime() - time, true);
                        return;
                    }
                    processFailed("Process migrate ledger failed", code, channel, feedback, RemotingException.of(
                            RemotingException.Failure.PROCESS_EXCEPTION,
                            "The broker is neither original broker nor destination broker"
                    ));
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                } catch (Throwable t) {
                    processFailed("Process migrate ledger failed", code, channel, feedback, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process migrate ledger failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the send message request, appending the record to the log, and handling the response feedback.
     *
     * @param channel  the channel through which the message is sent
     * @param code     the code representing the message type or command
     * @param data     the data buffer containing the message payload
     * @param feedback the feedback mechanism to handle the success or failure of the processing
     */
    protected void processSendMessage(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            SendMessageRequest request = readProto(data, SendMessageRequest.parser());
            int ledger = request.getLedger();
            int marker = request.getMarker();
            Promise<org.ephemq.common.message.Offset> promise = executor.newPromise();
            promise.addListener((GenericFutureListener<Future<org.ephemq.common.message.Offset>>) f -> {
                if (f.isSuccess()) {
                    try {
                        if (feedback != null) {
                            org.ephemq.common.message.Offset offset = f.getNow();
                            SendMessageResponse response = SendMessageResponse.newBuilder()
                                    .setLedger(ledger)
                                    .setEpoch(offset.getEpoch())
                                    .setIndex(offset.getIndex())
                                    .build();

                            feedback.success(proto2Buf(channel.alloc(), response));
                        }
                    } catch (Throwable t) {
                        processFailed("Process send message failed", code, channel, feedback, t);
                    }
                } else {
                    processFailed("Process send message failed", code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, f.isSuccess());
            });
            manager.getLogHandler().appendRecord(ledger, marker, data, promise);
        } catch (Throwable t) {
            processFailed("Process send message failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the query for cluster information, retrieves data about the cluster nodes,
     * constructs the cluster metadata, and sends a response back to the requester.
     *
     * @param channel the communication channel through which the request was received and through which the response will be sent.
     * @param code the specific command code identifying the type of request being processed.
     * @param data the ByteBuf containing the request data.
     * @param feedback the callback mechanism to send the response or error back to the requester.
     */
    protected void processQueryClusterInfo(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            commandExecutor.execute(() -> {
                try {
                    String clusterName = commonConfiguration.getClusterName();
                    List<org.ephemq.common.message.Node> clusterUpNodes =
                            manager.getClusterManager().getClusterReadyNodes();
                    Map<String, NodeMetadata> nodeMetadataMap = clusterUpNodes.stream().collect(
                            Collectors.toMap(org.ephemq.common.message.Node::getId, node ->
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
                    if (feedback != null) {
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process query cluster info failed", code, channel, feedback, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process query cluster info failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes query topic information requests received on the specified channel.
     *
     * @param channel the channel from which the request was received
     * @param code a unique code identifying the type of request
     * @param data a ByteBuf containing the serialized request data
     * @param feedback feedback mechanism to send the processed response
     */
    @SuppressWarnings("ConstantConditions")
    protected void processQueryTopicInfos(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
            ProtocolStringList topicNamesList = request.getTopicNamesList();
            commandExecutor.execute(() -> {
                try {
                    TopicHandleSupport support = manager.getTopicHandleSupport();
                    Set<String> topicNames = new HashSet<>();
                    if (topicNamesList.isEmpty()) {
                        topicNames.addAll(support.getAllTopics());
                    } else {
                        topicNames.addAll(topicNamesList);
                    }

                    QueryTopicInfoResponse.Builder builder = QueryTopicInfoResponse.newBuilder();
                    for (String topicName : topicNames) {
                        Set<PartitionInfo> partitionInfos = support.getTopicInfo(topicName);
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
                                    .setLeaderNodeId(info.getLeader())
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
                    if (feedback != null) {
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                } catch (Throwable t) {
                    processFailed("Process query topic info failed", code, channel, feedback, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process query topic info failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes a reset subscription request received over a REST channel.
     *
     * @param channel The channel through which the request was received.
     * @param code The operation code for identifying the type of request.
     * @param data The data buffer containing the serialized request.
     * @param feedback The feedback mechanism for sending responses back to the invoker.
     */
    protected void processRestSubscription(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            ResetSubscribeRequest request = readProto(data, ResetSubscribeRequest.parser());
            IntList markers = convertMarkers(request.getMarkers());
            int ledger = request.getLedger();
            int epoch = request.getEpoch();
            long index = request.getIndex();
            Promise<Integer> promise = executor.newPromise();
            promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                if (f.isSuccess()) {
                    if (feedback != null) {
                        ResetSubscribeResponse response = ResetSubscribeResponse.newBuilder().build();
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process reset subscription failed", code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogHandler().resetSubscribe(ledger, epoch, index, channel, markers, promise);
        } catch (Throwable t) {
            processFailed("Process reset subscription failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the alteration of a subscription by handling the provided data
     * and using the manager to alter the subscription details.
     *
     * @param channel  the channel through which data is being communicated
     * @param code     the operation code indicating the specific command to process
     * @param data     the data buffer containing the information needed for processing
     * @param feedback the feedback mechanism to communicate success or failure
     */
    protected void processAlterSubscription(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            AlterSubscribeRequest request = readProto(data, AlterSubscribeRequest.parser());
            IntList appendMarkers = convertMarkers(request.getAppendMarkers());
            IntList deleteMarkers = convertMarkers(request.getDeleteMarkers());
            int ledger = request.getLedger();
            Promise<Integer> promise = executor.newPromise();
            promise.addListener((GenericFutureListener<Future<Integer>>) f -> {
                if (f.isSuccess()) {
                    if (feedback != null) {
                        AlterSubscribeResponse response = AlterSubscribeResponse.newBuilder().build();
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process alter subscription failed", code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogHandler().alterSubscribe(channel, ledger, appendMarkers, deleteMarkers, promise);
        } catch (Throwable t) {
            processFailed("Process alter subscription failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Process the clean subscription request.
     *
     * @param channel The Netty channel through which the request was received.
     * @param code The command code associated with the request.
     * @param data The ByteBuf containing the request data.
     * @param feedback The feedback object used to send a response back to the client.
     */
    protected void processCleanSubscription(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CleanSubscribeRequest request = readProto(data, CleanSubscribeRequest.parser());
            int ledger = request.getLedger();
            Promise<Boolean> promise = executor.newPromise();
            promise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                if (f.isSuccess()) {
                    if (feedback != null) {
                        CleanSubscribeResponse response = CleanSubscribeResponse.newBuilder().build();
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                } else {
                    processFailed("Process clean subscription failed", code, channel, feedback, f.cause());
                }
                recordCommand(code, bytes, System.nanoTime() - time, false);
            });
            manager.getLogHandler().cleanSubscribe(channel, ledger, promise);
        } catch (Throwable t) {
            processFailed("Process clean subscription failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the creation of a topic in the given channel.
     *
     * @param channel The channel through which the request is received and response is sent.
     * @param code The code representing the operation being performed.
     * @param data Buffer containing the serialized CreateTopicRequest message.
     * @param feedback The feedback object used to send the result back to the client.
     */
    protected void processCreateTopic(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
            String topic = request.getTopic();
            int partition = request.getPartition();
            int replicas = request.getReplicas();
            CreateTopicConfigRequest configs = request.getConfigs();
            TopicConfig topicConfig = (configs.getSegmentRetainCount() == 0 || configs.getSegmentRetainMs() == 0 || configs.getSegmentRollingSize() == 0)
                    ? null : new TopicConfig(configs.getSegmentRollingSize(), configs.getSegmentRetainCount(), configs.getSegmentRetainMs(), configs.getAllocate());

            commandExecutor.execute(() -> {
                try {
                    TopicHandleSupport support = manager.getTopicHandleSupport();
                    Map<String, Object> createResult = support.createTopic(topic, partition, replicas, topicConfig);
                    if (feedback != null) {
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
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process create topic[" + topic + "] failed", code, channel, feedback, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process create topic failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Processes the request to delete a topic and provides feedback.
     *
     * @param channel  the channel through which the request is received
     * @param code     the operation code of the request
     * @param data     the data buffer containing the delete topic request
     * @param feedback the callback used to send the response back to the caller
     */
    protected void processDeleteTopic(Channel channel, int code, ByteBuf data, InvokedFeedback<ByteBuf> feedback) {
        long time = System.nanoTime();
        int bytes = data.readableBytes();
        try {
            DeleteTopicRequest request = readProto(data, DeleteTopicRequest.parser());
            String topic = request.getTopic();
            commandExecutor.execute(() -> {
                try {
                    manager.getTopicHandleSupport().deleteTopic(topic);
                    if (feedback != null) {
                        DeleteTopicResponse response = DeleteTopicResponse.newBuilder().build();
                        feedback.success(proto2Buf(channel.alloc(), response));
                    }
                    recordCommand(code, bytes, System.nanoTime() - time, true);
                } catch (Throwable t) {
                    processFailed("Process delete topic[" + topic + "] failed", code, channel, feedback, t);
                    recordCommand(code, bytes, System.nanoTime() - time, false);
                }
            });
        } catch (Throwable t) {
            processFailed("Process delete topic failed", code, channel, feedback, t);
            recordCommand(code, bytes, System.nanoTime() - time, false);
        }
    }

    /**
     * Records the execution of a command by notifying all registered API listeners with the details of the command execution.
     *
     * @param code The code identifying the command that was executed.
     * @param bytes The number of bytes processed during the execution of the command.
     * @param cost The time taken to execute the command, measured in nanoseconds.
     * @param ret The result of the command's execution; true if successful, false otherwise.
     */
    protected void recordCommand(int code, int bytes, long cost, boolean ret) {
        for (APIListener listener : manager.getAPIListeners()) {
            try {
                listener.onCommand(code, bytes, cost, ret);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Record process failed, listener[{}] code[{}]", listener == null ? null : listener.getClass().getSimpleName(), code, t);
                }
            }
        }
    }

    /**
     * Processes a failure event by logging an error and invoking the failure callback on the provided feedback object.
     *
     * @param err      the error message associated with the failure
     * @param code     the command code that was being processed when the failure occurred
     * @param channel  the network channel where the failure occurred
     * @param feedback a callback object used to signify the failure
     * @param throwable the exception that caused the failure
     */
    protected void processFailed(String err, int code, Channel channel, InvokedFeedback<ByteBuf> feedback, Throwable throwable) {
        if (feedback != null) {
            feedback.failure(throwable);
        }
        if (logger.isErrorEnabled()) {
            logger.error("Channel[{}] process failed, command[{}] and address[{}]", err, code, NetworkUtil.switchAddress(channel), throwable);
        }
    }

    /**
     * Converts ByteString markers into an IntList.
     *
     * @param markers the ByteString containing the markers to be converted
     * @return an IntList containing the converted markers
     * @throws Exception if any errors occur during conversion
     */
    protected IntList convertMarkers(ByteString markers) throws Exception {
        CodedInputStream stream = markers.newCodedInput();
        IntList markerList = new IntArrayList(markers.size() / 4);
        while (!stream.isAtEnd()) {
            markerList.add(stream.readFixed32());
        }
        return markerList;
    }
}
