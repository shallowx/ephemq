package org.shallow.producer;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.metadata.MessageRouter;
import org.shallow.metadata.MetadataManager;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.server.SendMessageRequest;
import org.shallow.proto.server.SendMessageResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.shallow.processor.ProcessCommand.Server.SEND_MESSAGE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class MessageProducer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageProducer.class);

    private final MetadataManager manager;
    private final ProducerConfig config;
    private final ShallowChannelPool pool;

    public MessageProducer(Client client, ProducerConfig config) {
        this.manager = client.getMetadataManager();
        this.config = config;
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
    }

    public void sendOneway(String topic, String queue, Message message) {
        sendOneway(topic, queue, message, null);
    }

    public SendResult send(String topic, String queue, Message message) throws Exception {
        return send(topic, queue, message, null);
    }

    public void sendAsync(String topic, String queue, Message message, SendCallback callback)  {
        sendAsync(topic, queue, message, null, callback);
    }

    public void sendOneway(String topic, String queue, Message message, MessageFilter messageFilter) {
        checkTopic(topic);
        checkQueue(queue);
        doSend(topic, queue, message, messageFilter, null);
    }

    public SendResult send(String topic, String queue, Message message, MessageFilter messageFilter) throws Exception {
        checkTopic(topic);
        checkQueue(queue);

        Promise<SendMessageResponse> promise = newImmediatePromise();
        doSend(topic, queue, message, messageFilter, promise);
        SendMessageResponse response = promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
        return new SendResult(response.getEpoch(), response.getIndex(), response.getLedger());
    }

    public void sendAsync(String topic, String queue, Message message, MessageFilter messageFilter, SendCallback callback)  {
        checkTopic(topic);
        checkQueue(queue);

        if (isNull(callback)) {
            doSend(topic, queue, message, messageFilter, null);
            return;
        }

        Promise<SendMessageResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<SendMessageResponse>>) future -> {
            if (future.isSuccess()) {
                SendMessageResponse response = future.get();
                callback.onCompleted(new SendResult(response.getEpoch(), response.getIndex(), response.getLedger()), null);
            } else {
                callback.onCompleted(null, future.cause());
            }
        });

        doSend(topic, queue, message, messageFilter, promise);
    }

    public void doSend(String topic, String queue, Message message, MessageFilter messageFilter, Promise<SendMessageResponse> promise) {

        MessageRouter messageRouter = manager.queryRouter(topic);
        if (isNull(messageRouter)) {
            throw new RuntimeException(String.format("Message router is empty, and topic=%s", topic));
        }

        MessageRouter.RouteHolder holder = messageRouter.allocRouteHolder(queue);
        SocketAddress leader = holder.leader();
        int ledger = holder.ledger();

        SendMessageRequest request = SendMessageRequest.newBuilder().build();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

        if (isNotNull(messageFilter)) {
            messageFilter.filter(topic, queue);
        }

        // TODO assemble message data
        clientChannel.invoker().invoke(SEND_MESSAGE, config.getInvokeExpiredMs(), promise, request, SendMessageResponse.class);
    }

    private void checkTopic(String topic) {
        if (isNull(topic) || topic.isEmpty()) {
            throw new IllegalArgumentException("Send topic cannot be empty");
        }
    }

    private void checkQueue(String queue) {
        if (isNull(queue) || queue.isEmpty()) {
            throw new IllegalArgumentException("Send queue cannot be empty");
        }
    }
}
