package org.shallow.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.*;
import org.shallow.internal.BrokerManager;
import org.shallow.log.Offset;
import org.shallow.remote.RemoteException;
import org.shallow.client.consumer.push.Subscription;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.remote.invoke.InvokeAnswer;
import org.shallow.log.LedgerManager;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.remote.processor.Ack;
import org.shallow.remote.processor.ProcessCommand;
import org.shallow.remote.processor.ProcessorAware;
import org.shallow.remote.proto.NodeMetadata;
import org.shallow.remote.proto.elector.*;
import org.shallow.remote.proto.server.*;
import static org.shallow.remote.util.NetworkUtil.*;
import static org.shallow.remote.util.ProtoBufUtil.proto2Buf;
import static org.shallow.remote.util.ProtoBufUtil.readProto;

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

                case PULL_MESSAGE -> processPullRequest(channel, data, answer);

                case HEARTBEAT -> processHeartbeatRequest(channel, data, answer);

                case SUBSCRIBE -> processSubscribeRequest(channel, data, answer);

                case CREATE_TOPIC -> processCreateTopicRequest(channel, command, data, answer, type, version);

                case DELETE_TOPIC -> processDelTopicRequest(channel, command, data, answer, type, version);

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

    private void processHeartbeatRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

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

    private void processRegisterNodeRequest(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer, byte type, short version) {
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

                        Promise<Void> promise = newImmediatePromise();
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

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
