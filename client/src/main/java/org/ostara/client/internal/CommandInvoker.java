package org.ostara.client.internal;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.Promise;
import org.ostara.remote.invoke.Callback;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.proto.MessageMetadata;
import org.ostara.remote.proto.server.*;
import org.ostara.remote.util.ByteBufUtils;
import org.ostara.remote.util.ProtoBufUtils;

public class CommandInvoker {
    private ClientChannel channel;

    public CommandInvoker(ClientChannel channel) {
        this.channel = channel;
    }

    public void sendMessage(int timeoutMs, Promise<SendMessageResponse> promise, SendMessageRequest request, MessageMetadata metadata, ByteBuf message){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, SendMessageResponse.parser());
            ByteBuf buf = assembleSendMessageData(channel.allocator(), request, metadata, message);
            channel.invoke(ProcessCommand.Server.SEND_MESSAGE, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void queryClusterInfo(int timeoutMs, Promise<QueryClusterResponse> promise, QueryClusterInfoRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, QueryClusterResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_CLUSTER_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void queryTopicInfo(int timeoutMs, Promise<QueryTopicInfoResponse> promise, QueryTopicInfoRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, QueryTopicInfoResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void resetSubscribe(int timeoutMs, Promise<ResetSubscribeResponse> promise, ResetSubscribeRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, ResetSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void alterSubscribe(int timeoutMs, Promise<AlterSubscribeResponse> promise, AlterSubscribeRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, AlterSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void cleanSubscribe(int timeoutMs, Promise<CleanSubscribeResponse> promise, CleanSubscribeRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, CleanSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void createTopic(int timeoutMs, Promise<CreateTopicResponse> promise, CreateTopicRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, CreateTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void deleteTopic(int timeoutMs, Promise<DeleteTopicResponse> promise, DeleteTopicRequest request){
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, DeleteTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t){
            tryFailure(promise, t);
        }
    }

    public void syncMessage(int timeoutMs, Promise<SyncResponse> promise, SyncRequest request) {
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, SyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.SYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    public void cancelSyncMessage(int timeoutMs, Promise<CancelSyncResponse> promise, CancelSyncRequest request) {
        try {
            Callback<ByteBuf> callback = assembleInvokeCallback(promise, CancelSyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(ProcessCommand.Server.UNSYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    private ByteBuf assembleSendMessageData(ByteBufAllocator allocator, SendMessageRequest request, MessageMetadata metadata, ByteBuf message) {
        ByteBuf data = null;
        try {
            int length = ProtoBufUtils.protoLength(request) + ProtoBufUtils.protoLength(metadata) + ByteBufUtils.bufLength(message);
            data = allocator.ioBuffer(length);

            ProtoBufUtils.writeProto(data, request);
            ProtoBufUtils.writeProto(data, metadata);
            if (message != null && message.isReadable()) {
                data.writeBytes(message, message.readerIndex(), message.readableBytes());
            }
            return data;
        } catch (Throwable t) {
            ByteBufUtils.release(data);
            throw new RuntimeException("Assemble send message failed");
        }
    }

    private ByteBuf assembleInvokeData(ByteBufAllocator allocator, MessageLite lite) {
        try {
            return ProtoBufUtils.proto2Buf(allocator, lite);
        } catch (Throwable t){
            String type = lite == null ? null : lite.getClass().getSimpleName();
            throw new RuntimeException("Assemble request data failed, type=" + type, t);
        }
    }

    private <T>Callback<ByteBuf> assembleInvokeCallback(Promise<T> promise, Parser<T> parser) {
        return promise == null ? null : (v, c) -> {
            if (c == null) {
                try {
                    promise.trySuccess(ProtoBufUtils.readProto(v, parser));
                } catch (Throwable t){
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
