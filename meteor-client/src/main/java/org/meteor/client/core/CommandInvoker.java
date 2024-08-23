package org.meteor.client.core;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.Promise;
import org.meteor.remote.invoke.Callable;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.server.AlterSubscribeRequest;
import org.meteor.remote.proto.server.AlterSubscribeResponse;
import org.meteor.remote.proto.server.CalculatePartitionsRequest;
import org.meteor.remote.proto.server.CalculatePartitionsResponse;
import org.meteor.remote.proto.server.CancelSyncRequest;
import org.meteor.remote.proto.server.CancelSyncResponse;
import org.meteor.remote.proto.server.CleanSubscribeRequest;
import org.meteor.remote.proto.server.CleanSubscribeResponse;
import org.meteor.remote.proto.server.CreateTopicRequest;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicRequest;
import org.meteor.remote.proto.server.DeleteTopicResponse;
import org.meteor.remote.proto.server.MigrateLedgerRequest;
import org.meteor.remote.proto.server.MigrateLedgerResponse;
import org.meteor.remote.proto.server.QueryClusterInfoRequest;
import org.meteor.remote.proto.server.QueryClusterResponse;
import org.meteor.remote.proto.server.QueryTopicInfoRequest;
import org.meteor.remote.proto.server.QueryTopicInfoResponse;
import org.meteor.remote.proto.server.ResetSubscribeRequest;
import org.meteor.remote.proto.server.ResetSubscribeResponse;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.proto.server.SyncRequest;
import org.meteor.remote.proto.server.SyncResponse;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;

public class CommandInvoker {
    private final ClientChannel channel;

    public CommandInvoker(ClientChannel channel) {
        this.channel = channel;
    }

    public void sendMessage(int timeoutMs, Promise<SendMessageResponse> promise, SendMessageRequest request,
                            MessageMetadata metadata, ByteBuf message) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, SendMessageResponse.parser());
            ByteBuf buf = assembleSendMessageData(channel.allocator(), request, metadata, message);
            channel.invoke(Command.Server.SEND_MESSAGE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void queryClusterInfo(int timeoutMs, Promise<QueryClusterResponse> promise, QueryClusterInfoRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, QueryClusterResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.QUERY_CLUSTER_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void queryTopicInfo(int timeoutMs, Promise<QueryTopicInfoResponse> promise, QueryTopicInfoRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, QueryTopicInfoResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void resetSubscribe(int timeoutMs, Promise<ResetSubscribeResponse> promise, ResetSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, ResetSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.REST_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void alterSubscribe(int timeoutMs, Promise<AlterSubscribeResponse> promise, AlterSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, AlterSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.ALTER_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void cleanSubscribe(int timeoutMs, Promise<CleanSubscribeResponse> promise, CleanSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CleanSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CLEAN_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void createTopic(int timeoutMs, Promise<CreateTopicResponse> promise, CreateTopicRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CreateTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CREATE_TOPIC, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void deleteTopic(int timeoutMs, Promise<DeleteTopicResponse> promise, DeleteTopicRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, DeleteTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.DELETE_TOPIC, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    public void syncMessage(int timeoutMs, Promise<SyncResponse> promise, SyncRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, SyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.SYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    public void cancelSyncMessage(int timeoutMs, Promise<CancelSyncResponse> promise, CancelSyncRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CancelSyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CANCEL_SYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    public void calculatePartitions(int timeoutMs, Promise<CalculatePartitionsResponse> promise, CalculatePartitionsRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CalculatePartitionsResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CALCULATE_PARTITIONS, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    public void migrateLedger(int timeoutMs, Promise<MigrateLedgerResponse> promise, MigrateLedgerRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, MigrateLedgerResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.MIGRATE_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    private ByteBuf assembleSendMessageData(ByteBufAllocator allocator, SendMessageRequest request,
                                            MessageMetadata metadata, ByteBuf message) {
        ByteBuf data = null;
        try {
            int length = ProtoBufUtil.protoLength(request) + ProtoBufUtil.protoLength(metadata) + ByteBufUtil.bufLength(message);
            data = allocator.ioBuffer(length);

            ProtoBufUtil.writeProto(data, request);
            ProtoBufUtil.writeProto(data, metadata);
            if (message != null && message.isReadable()) {
                data.writeBytes(message, message.readerIndex(), message.readableBytes());
            }
            return data;
        } catch (Throwable t) {
            ByteBufUtil.release(data);
            throw new RuntimeException("Assemble send message failed");
        }
    }

    private ByteBuf assembleInvokeData(ByteBufAllocator allocator, MessageLite lite) {
        try {
            return ProtoBufUtil.proto2Buf(allocator, lite);
        } catch (Throwable t) {
            String type = lite == null ? null : lite.getClass().getSimpleName();
            throw new RuntimeException(STR."Assemble request data failed, type=\{type}", t);
        }
    }

    private <T> Callable<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return promise == null ? null : (v, c) -> {
            if (c == null) {
                try {
                    promise.trySuccess(ProtoBufUtil.readProto(v, parser));
                } catch (Throwable t) {
                    promise.tryFailure(t);
                }
            } else {
                promise.tryFailure(c);
            }
        };
    }

    private void tryFailure(Promise<?> promise, Throwable t) {
        if (promise != null) {
            promise.tryFailure(t);
        }
    }
}
