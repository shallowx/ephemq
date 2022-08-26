package org.shallow.consumer.pull;

import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.metadata.MessageRouter;
import org.shallow.metadata.MessageRoutingHolder;
import org.shallow.metadata.MetadataManager;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.server.PullMessageRequest;
import org.shallow.proto.server.PullMessageResponse;
import org.shallow.util.ObjectUtil;

import java.net.SocketAddress;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNull;

public class MessagePullConsumer implements PullConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessagePullConsumer.class);

    private final MetadataManager manager;
    private final ConsumerConfig config;
    private final String name;
    private final ShallowChannelPool pool;
    private final Client client;
    private volatile boolean state = false;
    private final MessagePullListener listener;

    public MessagePullConsumer(ConsumerConfig config, String name, MessagePullListener pullListener) {
        this.client = new Client("consumer-client", config, new PullConsumerListener(this));
        this.manager = client.getMetadataManager();
        this.config = config;
        this.listener = pullListener;
        this.name = ObjectUtil.checkNonEmpty(name, "Message pull consumer name cannot be empty");
        this.pool = client.getChanelPool();
    }

    public void start() {
        if (state) {
            return;
        }
        state = true;
        client.start();
    }

    @Override
    public void pull(String topic, String queue, int limit) throws Exception {
        Promise<PullMessageResponse> promise = newImmediatePromise();
        doPullMessage(topic, queue, limit, promise);
    }

    @Override
    public MessagePullListener getPullListener() {
        return listener;
    }

    private void doPullMessage(String topic, String queue, int limit, Promise<PullMessageResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (isNull(messageRouter)) {
            throw new RuntimeException(String.format("The topic<%s> router is empty", topic));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        if (isNull(holder)) {
            throw new RuntimeException(String.format("The topic<%s> ledgers is empty", topic));
        }

        PullMessageRequest request = PullMessageRequest
                .newBuilder()
                .setQueue(queue)
                .setLimit(limit)
                .build();

        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);
        clientChannel.invoker().invoke(ProcessCommand.Server.PULL_MESSAGE, config.getInvokeExpiredMs(),promise, request, PullMessageResponse.class);
    }
}
