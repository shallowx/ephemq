package org.leopard.network;

import com.google.protobuf.ProtocolStringList;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import org.leopard.common.metadata.NodeRecord;
import org.leopard.common.metadata.PartitionRecord;
import org.leopard.internal.BrokerManager;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.Offset;
import org.leopard.remote.proto.NodeMetadata;
import org.leopard.remote.RemoteException;
import org.leopard.client.consumer.Subscription;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.remote.invoke.InvokeAnswer;
import org.leopard.ledger.LedgerManager;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.remote.processor.Ack;
import org.leopard.remote.processor.ProcessCommand;
import org.leopard.remote.processor.ProcessorAware;
import org.leopard.remote.proto.PartitionMetadata;
import org.leopard.remote.proto.TopicMetadata;
import org.leopard.remote.proto.server.*;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.leopard.remote.util.NetworkUtils.*;
import static org.leopard.remote.util.ProtoBufUtils.proto2Buf;
import static org.leopard.remote.util.ProtoBufUtils.readProto;

@SuppressWarnings("all")
public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerProcessorAware.class);

    private final BrokerConfig config;
    private final BrokerManager manager;
    private final EventExecutor commandExecutor;

    public BrokerProcessorAware(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;

        EventExecutorGroup group = newEventExecutorGroup(config.getProcessCommandHandleThreadLimit(), "processor-command").next();
        this.commandExecutor = group.next();
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

                case SUBSCRIBE -> processSubscribeRequest(channel, data, answer);

                case CREATE_TOPIC -> processCreateTopicRequest(channel, command, data, answer, type, version);

                case DELETE_TOPIC -> processDelTopicRequest(channel, command, data, answer, type, version);

                case FETCH_TOPIC_RECORD -> processFetchTopicRecordsRequest(channel, data, answer, version);

                case FETCH_CLUSTER_RECORD ->  processFetchClusterNodeRecordsRequest(channel, data, answer, version);

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

    private void processFetchTopicRecordsRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer, short version) {
        try {
            QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
            ProtocolStringList topicList = request.getTopicList();
            TopicPartitionRequestCacheWriterSupport topicPartitionCache = manager.getTopicPartitionCache();
            Set<PartitionRecord> records = topicPartitionCache.loadAll(topicList);

            if (records.isEmpty()) {
                answer.success(proto2Buf(channel.alloc(), QueryTopicInfoResponse.newBuilder().build()));
                return;
            }

            Map<Integer, PartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();
            Map<String, TopicMetadata> results = new ConcurrentHashMap<>();

            for (PartitionRecord partitionRecord : records) {
                PartitionMetadata partitionMetadata = PartitionMetadata.newBuilder()
                        .addAllReplicas(partitionRecord.getLatencies())
                        .setLeader(partitionRecord.getLeader())
                        .setId(partitionRecord.getId())
                        .setLatency(partitionRecord.getLatency())
                        .build();

                partitionMetadataMap.put(partitionMetadata.getId(), partitionMetadata);
            }

            TopicMetadata topicMetadata = TopicMetadata.newBuilder()
                    .setName(config.getClusterName())
                    .putAllPartitions(partitionMetadataMap)
                    .build();

            results.put(config.getClusterName(), topicMetadata);

          QueryTopicInfoResponse response = QueryTopicInfoResponse.newBuilder()
                .putAllTopics(results)
                .build();

          answer.success(proto2Buf(channel.alloc(), response));

        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processFetchClusterNodeRecordsRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer, short version) {
        try {
            QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
            String cluster = request.getCluster();
            ClusterNodeCacheWriterSupport cache = manager.getClusterCache();
            Set<NodeRecord> records = cache.load(cluster);

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
                            .setHost(((InetSocketAddress)socketAddress).getHostName())
                            .setPort(((InetSocketAddress)socketAddress).getPort())
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
                LedgerManager logManager = manager.getLedgerManager();
                logManager.append(ledger, queue, retain, version, promise);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to append message record", t);
                }
                answerFailed(answer,t);
            }
        } catch (Throwable t){
            if (logger.isErrorEnabled()) {
                logger.error("Failed to append message record", t);
            }
            answerFailed(answer,t);
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
                                    .setEpoch(subscription.getEpoch())
                                    .setLedger(ledger)
                                    .setIndex(subscription.getIndex())
                                    .setQueue(queue)
                                    .setVersion(subscription.getVersion())
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

                        TopicPartitionRequestCacheWriterSupport topicPartitionCache = manager.getTopicPartitionCache();
                        topicPartitionCache.createTopic(topic, partitions, latencies, promise);
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

    private void processDelTopicRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
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

                        TopicPartitionRequestCacheWriterSupport topicPartitionCache = manager.getTopicPartitionCache();
                        topicPartitionCache.delTopic(topic, promise);
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

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
