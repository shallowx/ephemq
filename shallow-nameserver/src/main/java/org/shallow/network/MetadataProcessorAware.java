package org.shallow.network;

import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.nraft.LeaderElector;
import org.shallow.proto.elector.RaftHeartbeatRequest;
import org.shallow.proto.elector.RaftHeartbeatResponse;
import org.shallow.proto.elector.VoteRequest;
import org.shallow.proto.elector.VoteResponse;
import org.shallow.proto.server.*;
import org.shallow.provider.ClusterMetadataProvider;
import org.shallow.internal.MetadataManager;
import org.shallow.RemoteException;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.NodeMetadata;
import org.shallow.provider.TopicMetadataProvider;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchAddress;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class MetadataProcessorAware implements ProcessorAware, ProcessCommand.NameServer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataProcessorAware.class);

    private final EventExecutor commandEventExecutor;
    private final TopicMetadataProvider topicMetadataProvider;
    private final ClusterMetadataProvider clusterMetadataProvider;
    private final LeaderElector leaderElector;

    public MetadataProcessorAware(MetadataManager metaManager) {
        this.commandEventExecutor = metaManager.commandEventExecutorGroup().next();
        this.topicMetadataProvider = metaManager.getTopicMetadataProvider();
        this.clusterMetadataProvider = metaManager.getClusterMetadataProvider();
        this.leaderElector = metaManager.getLeaderElector();
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("[onActive] - active channel<{}>", channel);
        }
        ProcessorAware.super.onActive(channel, executor);
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case NEW_TOPIC -> {
                    try {
                        final CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
                        commandEventExecutor.execute(() -> {
                            try {
                                final String topic = request.getTopic();
                                final int partitions = request.getPartitions();
                                final int latency = request.getLatency();

                                final Promise<CreateTopicResponse> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<CreateTopicResponse>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });

                                topicMetadataProvider.write2CacheAndFile(topic, partitions, latency, promise);
                            } catch (Throwable e) {
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case REMOVE_TOPIC -> {
                    try {
                        final DelTopicRequest request = readProto(data, DelTopicRequest.parser());
                        commandEventExecutor.execute(() -> {
                            try {
                                final String topic = request.getTopic();

                                final Promise<DelTopicResponse> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<DelTopicResponse>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), f.get()));
                                        }
                                    } else {
                                        answerFailed(answer, f.cause());
                                    }
                                });

                                topicMetadataProvider.delFromCache(topic, promise);
                            } catch (Throwable e) {
                                answerFailed(answer, e);
                            }
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case REGISTER_NODE -> {
                    try {
                        final RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final NodeMetadata node = request.getMetadata();
                            final Promise<RegisterNodeResponse> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<RegisterNodeResponse>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), f.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });

                            clusterMetadataProvider.write2CacheAndFile(request.getCluster(), node.getName(), node.getHost(), node.getPort(), promise);
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case HEARTBEAT -> {
                    try {
                        final RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final NodeMetadata node = request.getMetadata();
                            final Promise<HeartBeatResponse> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<HeartBeatResponse>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), f.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });
                            clusterMetadataProvider.keepHearBeat(request.getCluster(), node.getName(),node.getHost(), node.getPort(), promise);
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case QUERY_CLUSTER_IFO -> {
                    try {
                        final QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final String cluster = request.getCluster();
                            final Promise<QueryClusterNodeResponse> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<QueryClusterNodeResponse>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), f.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });
                            clusterMetadataProvider.queryActiveNodes(cluster, promise);
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case QUERY_TOPIC_INFO -> {
                    try {
                        final QueryTopicInfoRequest request = readProto(data, QueryTopicInfoRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final String topic = request.getTopic();
                            final Promise<QueryTopicInfoResponse> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<QueryTopicInfoResponse>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), f.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });
                            topicMetadataProvider.getTopicInfo(topic);
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case VOTE -> {
                    try {
                        final VoteRequest request = readProto(data, VoteRequest.parser());
                        final int term = request.getTerm();
                        commandEventExecutor.execute(() -> {
                            final Promise<? super MessageLite> promise = newImmediatePromise();
                            promise.addListener(f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        final VoteResponse response = (VoteResponse) f.get();
                                        answer.success(proto2Buf(channel.alloc(), response));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });

                            leaderElector.respond(term, promise);
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                case RAFT_HEARTBEAT -> {
                    try {
                        final RaftHeartbeatRequest request = readProto(data, RaftHeartbeatRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final Promise<RaftHeartbeatResponse> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<RaftHeartbeatResponse>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), f.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });
                            promise.trySuccess(RaftHeartbeatResponse.newBuilder().build());

                            leaderElector.keepHeartbeat();
                        });
                    } catch (Throwable cause) {
                        if (logger.isErrorEnabled()) {
                            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
                        }
                        answerFailed(answer, cause);
                    }
                }

                default -> {
                    if (logger.isErrorEnabled()) {
                        logger.error("[nameserver process]<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
    } catch (Throwable cause) {
        if (logger.isErrorEnabled()) {
            logger.error("[nameserver process]<{}> - command [{}] cause:{}", switchAddress(channel), command, cause);
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
