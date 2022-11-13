package org.leopard.nameserver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.leopard.common.meta.NodeRecord;
import org.leopard.nameserver.metadata.Manager;
import org.leopard.NameserverConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.nameserver.metadata.ClusterManager;
import org.leopard.nameserver.metadata.TopicManager;
import org.leopard.remote.proto.NodeMetadata;
import org.leopard.remote.proto.heartbeat.HeartbeatRequest;
import org.leopard.remote.proto.heartbeat.HeartbeatResponse;
import org.leopard.remote.RemoteException;
import org.leopard.remote.invoke.InvokeAnswer;
import org.leopard.remote.processor.Ack;
import org.leopard.remote.processor.ProcessCommand;
import org.leopard.remote.processor.ProcessorAware;
import org.leopard.remote.proto.server.*;
import org.leopard.remote.util.NetworkUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.leopard.remote.util.NetworkUtil.switchAddress;
import static org.leopard.remote.util.ProtoBufUtil.proto2Buf;
import static org.leopard.remote.util.ProtoBufUtil.readProto;

@SuppressWarnings("all")
public class NameserverProcessorAware implements ProcessorAware, ProcessCommand.Nameserver {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(NameserverProcessorAware.class);

    private final NameserverConfig config;
    private final Manager manager;

    public NameserverProcessorAware(NameserverConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
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
                case REGISTER_NODE -> processRegisterNodeRequest(channel, data, answer);
                case UN_REGISTER_NODE ->  processUnRegisterNodeRequest(channel, data, answer);

                case NEW_TOPIC -> processNewTopicRequest(channel, data, answer);

                case QUERY_NODE -> processQueryNodeRequest(channel, data, answer);

                case QUERY_TOPIC -> processQueryTopicRequest(channel, data, answer);

                case HEARTBEAT -> processHeartbeatRequest(channel, data, answer);

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

    private void processNewTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            Promise<Void> promise = NetworkUtil.newImmediatePromise();
            promise.addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (future.isSuccess()) {
                        if (answer != null) {
                            CreateTopicResponse response = CreateTopicResponse
                                    .newBuilder()
                                    .setAck(Ack.SUCCESS)
                                    .build();

                            answer.success(proto2Buf(channel.alloc(), response));
                        }
                    } else {
                        answerFailed(answer, future.cause());
                    }
                }
            });

            TopicManager topicManager = manager.getTopicManager();
            topicManager.add();
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processRegisterNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            NodeRegistrationRequest request = readProto(data, NodeRegistrationRequest.parser());
            String cluster = request.getCluster();
            String server = request.getServer();
            String host = request.getHost();
            int port = request.getPort();

            ClusterManager clusterManager = manager.getClusterManager();
            Promise<Void> promise = NetworkUtil.newImmediatePromise();
            promise.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        if (answer != null) {
                            NodeRegistrationResponse response = NodeRegistrationResponse.newBuilder().build();
                            answer.success(proto2Buf(channel.alloc(),response));
                        } else {
                            answerFailed(answer, future.cause());
                        }
                    }
                }
            });
            clusterManager.register(cluster, server, host, port, promise);
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    public void processUnRegisterNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            NodeUnregistrationRequest request = readProto(data, NodeUnregistrationRequest.parser());
            String cluster = request.getCluster();
            String server = request.getServer();

            ClusterManager clusterManager = manager.getClusterManager();
            Promise<Void> promise = NetworkUtil.newImmediatePromise();
            promise.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        if (answer != null) {
                            NodeUnregistrationResponse response = NodeUnregistrationResponse.newBuilder().build();
                            answer.success(proto2Buf(channel.alloc(), response));
                        } else {
                            answerFailed(answer, future.cause());
                        }
                    }
                }
            });

            clusterManager.unregister(cluster, server, promise);
        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void processQueryTopicRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

    }

    private void processQueryNodeRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            QueryClusterNodeRequest request = readProto(data, QueryClusterNodeRequest.parser());
            String cluster = request.getCluster();
            ClusterManager clusterManager = manager.getClusterManager();
            Set<NodeRecord> records = clusterManager.load(cluster);

            QueryClusterNodeResponse.Builder builder = QueryClusterNodeResponse.newBuilder();
            if (records == null || records.isEmpty()) {
                if (answer != null) {
                    answer.success(proto2Buf(channel.alloc(), builder.build()));
                }
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

    private void processHeartbeatRequest(Channel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
        try {
            HeartbeatRequest request = readProto(data, HeartbeatRequest.parser());
            String cluster = request.getCluster();
            String server = request.getServer();

            Promise<HeartbeatResponse> promise = NetworkUtil.newImmediatePromise();

            ClusterManager clusterManager = manager.getClusterManager();
            clusterManager.heartbeat(cluster, server);

            promise.trySuccess(HeartbeatResponse.newBuilder().build());

        } catch (Throwable t) {
            answerFailed(answer, t);
        }
    }

    private void answerFailed(InvokeAnswer<ByteBuf> answer, Throwable cause) {
        if (answer != null) {
            answer.failure(cause);
        }
    }
}
