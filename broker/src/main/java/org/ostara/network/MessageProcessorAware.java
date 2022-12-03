package org.ostara.network;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import static org.ostara.remote.util.NetworkUtils.newImmediatePromise;
import static org.ostara.remote.util.NetworkUtils.switchAddress;
import static org.ostara.remote.util.ProtoBufUtils.proto2Buf;
import static org.ostara.remote.util.ProtoBufUtils.readProto;
import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.common.metadata.Subscription;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.config.ServerConfig;
import org.ostara.internal.metadata.ClusterNodeCacheWriterSupport;
import org.ostara.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.ostara.ledger.LedgerEngine;
import org.ostara.ledger.Offset;
import org.ostara.remote.RemoteException;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.Ack;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.processor.ProcessorAware;
import org.ostara.remote.proto.NodeMetadata;
import org.ostara.remote.proto.PartitionMetadata;
import org.ostara.remote.proto.TopicMetadata;
import org.ostara.remote.proto.server.CreateTopicRequest;
import org.ostara.remote.proto.server.CreateTopicResponse;
import org.ostara.remote.proto.server.DelTopicRequest;
import org.ostara.remote.proto.server.DelTopicResponse;
import org.ostara.remote.proto.server.QueryClusterNodeRequest;
import org.ostara.remote.proto.server.QueryClusterNodeResponse;
import org.ostara.remote.proto.server.QueryTopicInfoRequest;
import org.ostara.remote.proto.server.QueryTopicInfoResponse;
import org.ostara.remote.proto.server.SendMessageRequest;
import org.ostara.remote.proto.server.SendMessageResponse;
import org.ostara.remote.proto.server.SubscribeRequest;
import org.ostara.remote.proto.server.SubscribeResponse;

@SuppressWarnings("all")
public class MessageProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageProcessorAware.class);

    private final ServerConfig config;
    private final ResourceContext resourceContext;
    private final EventExecutor commandExecutor;

    public MessageProcessorAware(ServerConfig config, ResourceContext resourceContext) {
        this.config = config;
        this.resourceContext = resourceContext;

        EventExecutorGroup group =
                newEventExecutorGroup(config.getProcessCommandHandleThreadLimit(), "processor-command").next();
        this.commandExecutor = group.next();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address<{}> successfully", channel.remoteAddress().toString());
        }

        resourceContext.getChannelBoundContext().add(channel);

        channel.closeFuture().addListener(f -> {
            LedgerEngine engine = resourceContext.getLedgerEngine();
            engine.clearChannel(channel);
        });
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type,
                        short version) {
        try {
            switch (command) {

                case SEND_MESSAGE -> processSendRequest(channel, data, answer, version);

                case SUBSCRIBE -> processSubscribeRequest(channel, data, answer);

                case CREATE_TOPIC -> processCreateTopicRequest(channel, command, data, answer, type, version);

                case DELETE_TOPIC -> processDelTopicRequest(channel, command, data, answer, type, version);

                case FETCH_TOPIC_RECORD -> processFetchTopicRecordsRequest(channel, data, answer, version);

                case FETCH_CLUSTER_RECORD -> processFetchClusterNodeRecordsRequest(channel, data, answer, version);

                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION,
                            "Not supported command [" + command + "]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("Channel<{}> - command [{}]", switchAddress(channel), command);
            }
            answerFailed(answer, cause);
        }
    }

    private void processFetchTopicRecordsRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer,
                                                 short version) {
        try {
            QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
            ProtocolStringList topicList = request.getTopicList();
            TopicPartitionRequestCacheWriterSupport writerSupport =
                    resourceContext.getPartitionRequestCacheWriterSupport();
            Map<String, Set<Partition>> partitions = writerSupport.loadAll(topicList);

            if (partitions.isEmpty()) {
                answer.success(proto2Buf(channel.alloc(), QueryTopicInfoResponse.newBuilder().build()));
                return;
            }

            Map<Integer, PartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();
            Map<String, TopicMetadata> results = new ConcurrentHashMap<>();

            Set<Map.Entry<String, Set<Partition>>> entries = partitions.entrySet();
            for (Map.Entry<String, Set<Partition>> entry : entries) {
                String topic = entry.getKey();
                Set<Partition> sets = entry.getValue();
                if (sets == null || sets.isEmpty()) {
                    continue;
                }

                for (Partition partition : sets) {
                    PartitionMetadata partitionMetadata = PartitionMetadata.newBuilder()
                            .addAllReplicas(partition.getReplicates())
                            .setLeader(partition.getLeader())
                            .setId(partition.getId())
                            .setLatency(partition.getLedgerId())
                            .build();

                    partitionMetadataMap.put(partitionMetadata.getId(), partitionMetadata);
                }

                TopicMetadata topicMetadata = TopicMetadata.newBuilder()
                        .setName(topic)
                        .putAllPartitions(partitionMetadataMap)
                        .build();

                results.put(topic, topicMetadata);
            }

            QueryTopicInfoResponse response = QueryTopicInfoResponse.newBuilder()
                    .putAllTopics(results)
                    .build();

            answer.success(proto2Buf(channel.alloc(), response));

        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processFetchClusterNodeRecordsRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer,
                                                       short version) {
        try {
            QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
            ClusterNodeCacheWriterSupport writerSupport = resourceContext.getNodeCacheWriterSupport();
            Set<Node> records = writerSupport.load(config.getClusterName());

            QueryClusterNodeResponse.Builder builder = QueryClusterNodeResponse.newBuilder();
            if (records == null || records.isEmpty()) {
                answer.success(proto2Buf(channel.alloc(), builder.build()));
            } else {
                List<NodeMetadata> metadatas = records.stream().map(record -> {
                    SocketAddress socketAddress = record.getSocketAddress();
                    return NodeMetadata.newBuilder()
                            .setName(record.getName())
                            .setCluster(record.getCluster())
                            .setState(record.getState())
                            .setHost(((InetSocketAddress) socketAddress).getHostName())
                            .setPort(((InetSocketAddress) socketAddress).getPort())
                            .build();
                }).collect(Collectors.toList());

                builder.addAllNodes(metadatas);

                if (answer != null) {
                    answer.success(proto2Buf(channel.alloc(), builder.build()));
                }
            }
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processSendRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer, short version) {
        try {
            SendMessageRequest request = readProto(data, SendMessageRequest.parser());

            ByteBuf retain = data.retain();
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
                LedgerEngine engine = resourceContext.getLedgerEngine();
                engine.append(ledger, queue, retain, version, promise);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to append message record", t);
                }
                answerFailed(answer, t);
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to append message record", t);
            }
            answerFailed(answer, t);
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
                            Subscription subscription = (Subscription) future.get();
                            SubscribeResponse response = SubscribeResponse
                                    .newBuilder()
                                    .setEpoch(subscription.getEpoch())
                                    .setLedger(ledger)
                                    .setIndex(subscription.getIndex())
                                    .setQueue(queue)
                                    .setVersion(subscription.getVersion())
                                    .build();

                            if (answer != null) {
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            answerFailed(answer, future.cause());
                        }
                    });

                    LedgerEngine engine = resourceContext.getLedgerEngine();
                    engine.subscribe(channel, topic, queue, theVersion, ledger, epoch, index, promise);
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

    private void processCreateTopicRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer,
                                           byte type, short version) {
        try {
            CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
            commandExecutor.execute(() -> {
                try {
                    String topic = request.getTopic();
                    int partitionLimit = request.getPartitionLimit();
                    int replicateLimit = request.getReplicateLimit();

                    if (logger.isDebugEnabled()) {
                        logger.debug("The topic<{}> partitions<{}> latency<{}>", topic, partitionLimit, replicateLimit);
                    }

                    Promise<Void> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<Void>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
                                CreateTopicResponse response = CreateTopicResponse
                                        .newBuilder()
                                        .setTopic(topic)
                                        .setPartitionLimit(partitionLimit)
                                        .setReplicateLimit(replicateLimit)
                                        .setAck(Ack.SUCCESS)
                                        .build();
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            answerFailed(answer, f.cause());
                        }
                    });

                    TopicPartitionRequestCacheWriterSupport writerSupport =
                            resourceContext.getPartitionRequestCacheWriterSupport();
                    writerSupport.createTopic(topic, partitionLimit, replicateLimit, promise);
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to create topic with address<{}>, cause:{}",
                                channel.remoteAddress().toString(), e);
                    }
                    answerFailed(answer, e);
                }
            });
        } catch (Exception e) {
            answerFailed(answer, e);
        }
    }

    private void processDelTopicRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer,
                                        byte type, short version) {
        try {
            DelTopicRequest request = readProto(data, DelTopicRequest.parser());
            String topic = request.getTopic();

            commandExecutor.execute(() -> {
                try {
                    Promise<Void> promise = newImmediatePromise();
                    promise.addListener((GenericFutureListener<Future<Void>>) f -> {
                        if (f.isSuccess()) {
                            if (answer != null) {
                                DelTopicResponse response = DelTopicResponse.newBuilder().build();
                                answer.success(proto2Buf(channel.alloc(), response));
                            }
                        } else {
                            answerFailed(answer, f.cause());
                        }
                    });

                    TopicPartitionRequestCacheWriterSupport writerSupport =
                            resourceContext.getPartitionRequestCacheWriterSupport();
                    writerSupport.delTopic(topic, promise);
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to delete topic<{}> with address<{}>, cause:{}", topic,
                                channel.remoteAddress().toString(), e);
                    }
                    answerFailed(answer, e);
                }
            });
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to delete topic with address<{}>, cause:{}", channel.remoteAddress().toString(),
                        e);
            }
            answerFailed(answer, e);
        }
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
