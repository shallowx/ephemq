package org.shallow.network;

import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.provider.ClusterMetadataProvider;
import org.shallow.internal.MetadataConfig;
import org.shallow.internal.MetadataManager;
import org.shallow.RemoteException;
import org.shallow.invoke.InvokeAnswer;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.CreateTopicRequest;
import org.shallow.proto.server.DelTopicRequest;
import org.shallow.proto.server.RegisterNodeRequest;
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

    public MetadataProcessorAware(MetadataConfig config, MetadataManager metaManager) {
        this.commandEventExecutor = metaManager.commandEventExecutorGroup().next();
        this.topicMetadataProvider = metaManager.getTopicMetadataProvider();
        this.clusterMetadataProvider = metaManager.getClusterMetadataProvider();
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

                                if (logger.isDebugEnabled()) {
                                    logger.debug("[meta server process] - topic<{}> partitions<{}> latency<{}>", topic, partitions, latency);
                                }

                                Promise<MessageLite> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<Object>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), promise.get()));
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
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }

                case REMOVE_TOPIC -> {
                    try {
                        final DelTopicRequest request = readProto(data, DelTopicRequest.parser());
                        commandEventExecutor.execute(() -> {
                            try {
                                final String topic = request.getTopic();

                                Promise<MessageLite> promise = newImmediatePromise();
                                promise.addListener((GenericFutureListener<Future<Object>>) f -> {
                                    if (f.isSuccess()) {
                                        if (isNotNull(answer)) {
                                            answer.success(proto2Buf(channel.alloc(), promise.get()));
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
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }

                case REGISTER_NODE -> {
                    try {
                        final RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final NodeMetadata node = request.getMetadata();
                            Promise<MessageLite> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<Object>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), promise.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });

                            clusterMetadataProvider.write2CacheAndFile(request.getCluster(), node.getName(), node.getHost(), node.getPort(), promise);
                        });
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }

                case HEART_BEAT -> {
                    try {
                        final RegisterNodeRequest request = readProto(data, RegisterNodeRequest.parser());
                        commandEventExecutor.execute(() -> {
                            final NodeMetadata node = request.getMetadata();
                            Promise<MessageLite> promise = newImmediatePromise();
                            promise.addListener((GenericFutureListener<Future<Object>>) f -> {
                                if (f.isSuccess()) {
                                    if (isNotNull(answer)) {
                                        answer.success(proto2Buf(channel.alloc(), promise.get()));
                                    }
                                } else {
                                    answerFailed(answer, f.cause());
                                }
                            });
                            clusterMetadataProvider.keepHearBeat(request.getCluster(), node.getName(),node.getHost(), node.getPort(), promise);
                        });
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }
                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[nameserver process]<{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
    } catch (Throwable cause) {
        if (logger.isErrorEnabled()) {
            logger.error("[nameserver process]<{}> - command [{}]", switchAddress(channel), command);
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
