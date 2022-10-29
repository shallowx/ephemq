package org.shallow.example.quickstart;

import org.junit.jupiter.api.Test;
import org.shallow.client.ClientConfig;
import org.shallow.client.Message;
import org.shallow.client.consumer.ConsumerConfig;
import org.shallow.client.consumer.push.MessagePushConsumer;
import org.shallow.client.consumer.push.MessagePushListener;
import org.shallow.client.consumer.push.Subscription;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

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

        org.shallow.client.consumer.push.PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
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
