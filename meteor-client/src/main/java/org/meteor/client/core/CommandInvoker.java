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

/**
 * CommandInvoker is responsible for sending various types of commands via the ClientChannel.
 * It handles message communication, cluster queries, topic and subscription management,
 * synchronization of messages, and ledger operations.
 */
public class CommandInvoker {
    /**
     * Represents the client channel used for communication in {@link CommandInvoker}.
     * <p>
     * This is a final instance of {@code ClientChannel} which is responsible for
     * handling all the interaction between the client and server.
     */
    private final ClientChannel channel;

    /**
     * Initializes a new instance of the CommandInvoker with the given ClientChannel.
     *
     * @param channel The ClientChannel through which commands will be sent.
     */
    public CommandInvoker(ClientChannel channel) {
        this.channel = channel;
    }

    /**
     * Sends a message using the specified parameters.
     *
     * @param timeoutMs the timeout in milliseconds for the send operation.
     * @param promise   the promise to be completed with the result of send operation.
     * @param request   the details of the send message request.
     * @param metadata  the metadata associated with the message.
     * @param message   the message content in ByteBuf format.
     */
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

    /**
     * Queries information about the cluster.
     *
     * @param timeoutMs the timeout in milliseconds for the query operation
     * @param promise the promise to be fulfilled with the query result
     * @param request the details of the cluster info query request
     */
    public void queryClusterInfo(int timeoutMs, Promise<QueryClusterResponse> promise, QueryClusterInfoRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, QueryClusterResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.QUERY_CLUSTER_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Queries topic information by sending a request to the server and handling the response.
     *
     * @param timeoutMs the maximum time to wait for the server response, in milliseconds
     * @param promise   the promise to hold the response or any errors encountered during the process
     * @param request   the request object containing the details of the topic to query
     */
    public void queryTopicInfo(int timeoutMs, Promise<QueryTopicInfoResponse> promise, QueryTopicInfoRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, QueryTopicInfoResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.QUERY_TOPIC_INFOS, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Reset the subscription for a given request, utilizing a specified timeout.
     *
     * @param timeoutMs the timeout in milliseconds for the reset subscription operation.
     * @param promise the promise object to hold the response of type ResetSubscribeResponse.
     * @param request the request object containing the data needed to reset the subscription.
     */
    public void resetSubscribe(int timeoutMs, Promise<ResetSubscribeResponse> promise, ResetSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, ResetSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.REST_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Alters the subscription settings based on the provided request.
     *
     * @param timeoutMs the timeout duration in milliseconds.
     * @param promise the promise that will be completed when the operation finishes.
     * @param request the request object containing the subscription alteration details.
     */
    public void alterSubscribe(int timeoutMs, Promise<AlterSubscribeResponse> promise, AlterSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, AlterSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.ALTER_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Initiates a clean subscribe request with the specified timeout, handling the response asynchronously.
     *
     * @param timeoutMs the maximum time to wait for the response, in milliseconds
     * @param promise   the promise to be completed with the response or an error
     * @param request   the clean subscribe request to be sent
     */
    public void cleanSubscribe(int timeoutMs, Promise<CleanSubscribeResponse> promise, CleanSubscribeRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CleanSubscribeResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CLEAN_SUBSCRIBE, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Creates a topic on the server.
     *
     * @param timeoutMs the maximum time to wait for a response, in milliseconds
     * @param promise the promise to be fulfilled with the response
     * @param request the request containing the details for creating the topic
     */
    public void createTopic(int timeoutMs, Promise<CreateTopicResponse> promise, CreateTopicRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CreateTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CREATE_TOPIC, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Deletes a specified topic within a timeout period.
     *
     * @param timeoutMs the maximum time in milliseconds to wait for the delete operation to complete
     * @param promise a promise object to receive the response or error for the delete operation
     * @param request a request object containing the details of the topic to be deleted
     */
    public void deleteTopic(int timeoutMs, Promise<DeleteTopicResponse> promise, DeleteTopicRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, DeleteTopicResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.DELETE_TOPIC, buf, timeoutMs, callback);
        } catch (Throwable t) {
            tryFailure(promise, t);
        }
    }

    /**
     * Synchronizes a message with the server by sending a sync request and waiting for a response
     * within a specified timeout period.
     *
     * @param timeoutMs the maximum time to wait for a response, in milliseconds
     * @param promise the promise to be fulfilled with the sync response
     * @param request the sync request to be sent to the server
     */
    public void syncMessage(int timeoutMs, Promise<SyncResponse> promise, SyncRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, SyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.SYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    /**
     * Cancels a synchronous message invocation.
     *
     * @param timeoutMs The timeout in milliseconds for the operation.
     * @param promise The promise object to handle the response or error.
     * @param request The request containing the details needed to cancel the sync message.
     */
    public void cancelSyncMessage(int timeoutMs, Promise<CancelSyncResponse> promise, CancelSyncRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CancelSyncResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CANCEL_SYNC_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    /**
     * This method calculates partitions by sending a request to the server
     * and handling the result asynchronously.
     *
     * @param timeoutMs the maximum time to wait for the response in milliseconds
     * @param promise the promise object to handle the asynchronous result
     * @param request the calculate partitions request object that contains
     *                the necessary data for the request
     */
    public void calculatePartitions(int timeoutMs, Promise<CalculatePartitionsResponse> promise, CalculatePartitionsRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, CalculatePartitionsResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.CALCULATE_PARTITIONS, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    /**
     * Initiates a ledger migration request to the server.
     *
     * @param timeoutMs the timeout for the operation in milliseconds
     * @param promise the promise to be completed with the result of the migration operation
     * @param request the request containing the ledger migration details
     */
    public void migrateLedger(int timeoutMs, Promise<MigrateLedgerResponse> promise, MigrateLedgerRequest request) {
        try {
            Callable<ByteBuf> callback = assembleInvokeCallback(promise, MigrateLedgerResponse.parser());
            ByteBuf buf = assembleInvokeData(channel.allocator(), request);
            channel.invoke(Command.Server.MIGRATE_LEDGER, buf, timeoutMs, callback);
        } catch (Exception e) {
            tryFailure(promise, e);
        }
    }

    /**
     * Assembles and returns the data required for sending a message.
     *
     * @param allocator the ByteBufAllocator to allocate buffer for message data
     * @param request the SendMessageRequest containing the message request details
     * @param metadata the MessageMetadata containing additional metadata for the message
     * @param message the ByteBuf containing the message content
     * @return a ByteBuf containing the assembled message data
     */
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

    /**
     * Assembles a protocol buffer message into a ByteBuf.
     *
     * @param allocator The ByteBufAllocator used to allocate new ByteBuf.
     * @param lite The protocol buffer message to be converted.
     * @return A ByteBuf containing the serialized data of the provided protocol buffer message.
     * @throws RuntimeException If the assembly of the request data fails.
     */
    private ByteBuf assembleInvokeData(ByteBufAllocator allocator, MessageLite lite) {
        try {
            return ProtoBufUtil.proto2Buf(allocator, lite);
        } catch (Throwable t) {
            String type = lite == null ? null : lite.getClass().getSimpleName();
            throw new RuntimeException(STR."Assemble request data failed, type=\{type}", t);
        }
    }

    /**
     * Assembles a callback for promise resolution upon invocation completion.
     *
     * @param <T>    the type of the result.
     * @param promise the promise to be fulfilled upon task completion.
     * @param parser  the parser used to decode the response.
     * @return a {@code Callable} that processes the invocation result and fulfills the promise.
     */
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

    /**
     * Attempts to mark the provided promise as failed if it is not null.
     *
     * @param promise the Promise object to be marked as failed
     * @param t the Throwable that indicates the reason for failure
     */
    private void tryFailure(Promise<?> promise, Throwable t) {
        if (promise != null) {
            promise.tryFailure(t);
        }
    }
}
