package org.shallow.network;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.RemoteException;
import org.shallow.internal.BrokerManager;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.TopicManager;
import org.shallow.metadata.sraft.SRaftLog;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.elector.RaftHeartbeatRequest;
import org.shallow.proto.elector.VoteRequest;
import org.shallow.proto.elector.VoteResponse;
import org.shallow.proto.server.*;

import static org.shallow.util.NetworkUtil.*;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerManager manager;
    private final EventExecutor commandExecutor;

    public BrokerProcessorAware(BrokerManager manager) {
        this.manager = manager;
        this.commandExecutor = newEventExecutorGroup(1, "procesor-aware").next();
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
                case QUORUM_VOTE -> {
                    try {
                        VoteRequest request = readProto(data, VoteRequest.parser());
                        commandExecutor.execute(() -> {
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
                        answerFailed(answer, e);
                    }
                }

                case HEARTBEAT -> {
                    try {
                        RaftHeartbeatRequest request = readProto(data, RaftHeartbeatRequest.parser());
                        commandExecutor.execute(() -> {
                            try {
                                int term = request.getTerm();
                                SRaftProcessController controller = manager.getController();
                                controller.receiveHeartbeat(term);
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

                case CREATE_TOPIC -> {
                    try {
                        CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
                        String topic = request.getTopic();

                        commandExecutor.execute(() -> {
                            try {
                                int partitions = request.getPartitions();
                                int latencies = request.getLatencies();

                                if (logger.isDebugEnabled()) {
                                    logger.debug("The topic<{}> partitions<{}> latency<{}>", topic, partitions, latencies);
                                }

                                Promise<CreateTopicResponse> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<CreateTopicResponse>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });
                            } catch (Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Failed to create topic<{}> with address<{}>, cause:{}", topic, channel.remoteAddress().toString(), e);
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
                                Promise<DelTopicResponse> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<DelTopicResponse>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
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
                        answerFailed(answer, e);
                    }
                }

                case FETCH_CLUSTER_RECORD -> {

                }

                case FETCH_TOPIC_RECORD -> {}

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
