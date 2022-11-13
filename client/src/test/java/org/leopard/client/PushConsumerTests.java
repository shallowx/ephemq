package org.leopard.client;

import org.leopard.client.consumer.push.*;
import org.junit.Test;
import org.leopard.client.consumer.ConsumerConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class PushConsumerTests {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerTests.class);

    @Test
    public void subscribe1() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessagePushListener() {
            @Override
            public void onMessage(Message message) {
                if (logger.isInfoEnabled()) {
                    logger.info("Recive message:{}", message);
                }
            }
        });
        messagePushConsumer.start();

        Subscription subscribe = messagePushConsumer.subscribe("test", "message");
        latch.await();

        messagePushConsumer.shutdownGracefully();
    }

    @Test
    public void subscribeAsync() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        PushConsumer messagePushConsumer = new MessagePushConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessagePushListener() {
            @Override
            public void onMessage(Message message) {
                if (logger.isInfoEnabled()) {
                    logger.info("Recive message:{}", message);
                }

            }
        });
        messagePushConsumer.start();

        messagePushConsumer.subscribeAsync("create", "message", new SubscribeCallback() {
            @Override
            public void onCompleted(Subscription subscription, Throwable cause) {
                    if (cause != null) {
                        if (logger.isErrorEnabled()) {
                            logger.error(cause);
                        }
                        return;
                    }

                    if (logger.isInfoEnabled()) {
                        logger.info("subscription ship:{}", subscription);
                    }
            }
        });
        latch.await();

        messagePushConsumer.shutdownGracefully();
    }
}
