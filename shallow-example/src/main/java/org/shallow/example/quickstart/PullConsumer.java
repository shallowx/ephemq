package org.shallow.example.quickstart;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;
import org.shallow.ClientConfig;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.consumer.pull.MessagePullConsumer;
import org.shallow.consumer.pull.MessagePullListener;
import org.shallow.consumer.pull.PullResult;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.server.PullMessageResponse;
import org.shallow.util.NetworkUtil;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class PullConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PullConsumer.class);

    @Test
    public void pull() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        org.shallow.consumer.pull.PullConsumer messagePullConsumer = new MessagePullConsumer(consumerConfig, "pull-consumer");
        messagePullConsumer.registerListener(new MessagePullListener() {
            @Override
            public void onMessage(PullResult result) {
                if (logger.isInfoEnabled()) {
                    logger.info("Message pull result:" + result);
                }
                latch.countDown();
            }
        });
        messagePullConsumer.start();

        Promise<PullMessageResponse> promise = NetworkUtil.newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<PullMessageResponse>>) future -> {
            if (future.isSuccess()) {
                PullMessageResponse response = future.get();
                if (logger.isInfoEnabled()) {
                    logger.info("Send pull message callback result: ledger={} epoch={} index={} limit={} queue={} topic={}",
                            response.getLedger(), response.getEpoch(), response.getIndex(), response.getLimit(), response.getQueue(), response.getTopic());
                }
            } else {
                if (logger.isErrorEnabled()) {
                    logger.error(future.cause());
                }
            }
        });
        messagePullConsumer.pull("create", "message", -1, -1,3, promise);

        latch.await();
        messagePullConsumer.shutdownGracefully();
    }
}
