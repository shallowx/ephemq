package org.shallow.consumer.pull;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
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
import static org.shallow.util.ObjectUtil.isNull;

public class MessagePullConsumer implements PullConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessagePullConsumer.class);

    private MetadataManager manager;
    private final ConsumerConfig config;
    private final String name;
    private ShallowChannelPool pool;
    private final Client client;
    private volatile boolean state = false;
    private final MessagePullListener listener;

    public MessagePullConsumer(ConsumerConfig config, String name, MessagePullListener pullListener) {
        this.listener = pullListener;
        this.client = new Client("consumer-client", config.getClientConfig(), new PullConsumerListener(this));
        this.config = config;
        this.name = ObjectUtil.checkNonEmpty(name, "Message pull consumer name cannot be empty");
    }

    public void start() {
        if (state) {
            return;
        }
        state = true;
        client.start();

        this.manager = client.getMetadataManager();
        this.pool = client.getChanelPool();
    }

    @Override
    public void pull(String topic, String queue, int epoch, long index, int limit, Promise<PullMessageResponse> promise) throws Exception {
        promise.addListener((GenericFutureListener<Future<PullMessageResponse>>) future -> {
            if (future.isSuccess()) {
                promise.trySuccess(future.get());
            } else {
                promise.tryFailure(future.cause());
            }
        });
        doPullMessage(topic, queue, epoch, index, limit, promise);
    }

    @Override
    public MessagePullListener getPullListener() {
        return listener;
    }

    private void doPullMessage(String topic, String queue,int epoch, long index, int limit, Promise<PullMessageResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (isNull(messageRouter)) {
            throw new RuntimeException(String.format("The topic<%s> router is empty", topic));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        if (isNull(holder)) {
            throw new RuntimeException(String.format("The topic<%s> ledgers is empty", topic));
        }
        int ledger = holder.ledger();

        PullMessageRequest request = PullMessageRequest
                .newBuilder()
                .setLedger(ledger)
                .setEpoch(epoch)
                .setIndex(index)
                .setQueue(queue)
                .setLimit(limit)
                .build();

        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);
        clientChannel.invoker().invoke(ProcessCommand.Server.PULL_MESSAGE, config.getClientConfig().getInvokeExpiredMs(),promise, request, PullMessageResponse.class);
    }
}
