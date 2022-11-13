package org.leopard.example.quickstart;

import org.junit.jupiter.api.Test;
import org.leopard.client.ClientConfig;
import org.leopard.client.Message;
import org.leopard.client.consumer.ConsumerConfig;
import org.leopard.client.consumer.push.MessagePushConsumer;
import org.leopard.client.consumer.push.MessagePushListener;
import org.leopard.client.consumer.push.Subscription;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class PushConsumer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PullConsumer.class);

    @Test
    public void subscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        org.leopard.client.consumer.push.PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessagePushListener() {
            @Override
            public void onMessage(Message message) {
                if (logger.isInfoEnabled()) {
                    logger.info("Receive message:{}", message);
                }
            }
        });
        messagePushConsumer.start();

        Subscription subscribe = messagePushConsumer.subscribe("create", "message");
        latch.await(60, TimeUnit.SECONDS);

        messagePushConsumer.shutdownGracefully();
    }
}
