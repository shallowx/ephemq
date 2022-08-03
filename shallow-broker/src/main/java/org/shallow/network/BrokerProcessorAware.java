package org.shallow.network;

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
import org.shallow.metadata.Topic2NameserverManager;
import org.shallow.processor.ProcessCommand;
import org.shallow.processor.ProcessorAware;
import org.shallow.proto.server.CreateTopicRequest;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicRequest;
import org.shallow.proto.server.DelTopicResponse;

import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchAddress;
import static org.shallow.util.ProtoBufUtil.proto2Buf;
import static org.shallow.util.ProtoBufUtil.readProto;

public class BrokerProcessorAware implements ProcessorAware, ProcessCommand.Server {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BrokerSocketServer.class);

    private final BrokerManager manager;

    public BrokerProcessorAware(BrokerManager manager) {
        this.manager = manager;
    }

    @Override
    public void onActive(Channel channel, EventExecutor executor) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get remote active address <{}>", channel.remoteAddress());
        }
    }

    @Override
    public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            switch (command) {
                case CREATE_TOPIC -> {
                    try {
                        final CreateTopicRequest request = readProto(data, CreateTopicRequest.parser());
                        final String topic = request.getTopic();
                        final int partitions = request.getPartitions();
                        final int latency = request.getLatency();

                        if (logger.isDebugEnabled()) {
                            logger.debug("[broker server process] - topic<{}> partitions<{}> latency<{}>", topic, partitions, latency);
                        }

                        Promise<CreateTopicResponse> promise = newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<Object>>) f -> {
                            if (f.isSuccess()) {
                                if (isNotNull(answer)) {
                                    answer.success(proto2Buf(channel.alloc(), promise.get()));
                                }
                            } else {
                                answerFailed(answer, f.cause());
                            }
                        });

                        Topic2NameserverManager topic2NameserverManager = manager.getTopic2NameserverManager();
                        topic2NameserverManager.write2Nameserver(topic, partitions, latency, promise);
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }
                case DELETE_TOPIC -> {
                    try {
                        final DelTopicRequest request = readProto(data, DelTopicRequest.parser());
                        final String topic = request.getTopic();

                        Promise<DelTopicResponse> promise = newImmediatePromise();
                        promise.addListener((GenericFutureListener<Future<DelTopicResponse>>) f -> {
                            if (f.isSuccess()) {
                                if (isNotNull(answer)) {
                                    answer.success(proto2Buf(channel.alloc(), promise.get()));
                                }
                            } else {
                                answerFailed(answer, f.cause());
                            }
                        });

                        Topic2NameserverManager topic2NameserverManager = manager.getTopic2NameserverManager();
                        topic2NameserverManager.delFormNameserver(topic, promise);
                    } catch (Exception e) {
                        answerFailed(answer, e);
                    }
                }
                case FETCH_CLUSTER_INFO -> {

                }
                case FETCH_TOPIC_INFO -> {}
                default -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[broker server process] <{}> - not supported command [{}]", switchAddress(channel), command);
                    }
                    answerFailed(answer, RemoteException.of(RemoteException.Failure.UNSUPPORTED_EXCEPTION, "Not supported command ["+ command +"]"));
                }
            }
        } catch (Throwable cause) {
            if (logger.isErrorEnabled()) {
                logger.error("[broker server process]<{}> - command [{}]", switchAddress(channel), command);
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
