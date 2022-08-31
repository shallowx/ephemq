package org.shallow;

import org.junit.Test;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.consumer.push.MessagePushConsumer;
import org.shallow.consumer.push.MessagePushListener;
import org.shallow.consumer.push.Subscription;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("all")
public class PushConsumerTests {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerTests.class);

    @Test
    public void subscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        org.shallow.consumer.push.PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessagePushListener() {
            @Override
            public void onMessage(Message message) {
                if (logger.isInfoEnabled()) {
                    logger.info("Recive message:{}", message);
                }
            }
        });
        messagePushConsumer.start();

        Subscription subscribe = messagePushConsumer.subscribe("create", "message");
        latch.await(60, TimeUnit.SECONDS);

        messagePushConsumer.shutdownGracefully();
    }
}
