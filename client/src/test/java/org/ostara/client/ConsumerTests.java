package org.ostara.client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.ostara.client.consumer.Consumer;
import org.ostara.client.consumer.ConsumerConfig;
import org.ostara.client.consumer.MessageConsumer;
import org.ostara.client.consumer.MessageListener;
import org.ostara.client.consumer.SubscribeCallback;
import org.ostara.client.internal.ClientConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Subscription;

@SuppressWarnings("all")
public class ConsumerTests {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerTests.class);

    @Test
    public void subscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer messagePushConsumer = new MessageConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessageListener() {
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

        Consumer messagePushConsumer = new MessageConsumer("example-consumer", consumerConfig);
        messagePushConsumer.registerListener(new MessageListener() {
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
