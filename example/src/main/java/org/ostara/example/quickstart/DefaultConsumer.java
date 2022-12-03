package org.ostara.example.quickstart;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.ostara.client.Message;
import org.ostara.client.consumer.Consumer;
import org.ostara.client.consumer.ConsumerConfig;
import org.ostara.client.consumer.MessageConsumer;
import org.ostara.client.consumer.MessageListener;
import org.ostara.client.internal.ClientConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Subscription;

@SuppressWarnings("all")
public class DefaultConsumer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumer.class);

    @Test
    public void subscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer messageConsumer = new MessageConsumer("example-consumer", consumerConfig);
        messageConsumer.registerListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                if (logger.isInfoEnabled()) {
                    logger.info("Receive message:{}", message);
                }
            }
        });
        messageConsumer.start();

        Subscription subscribe = messageConsumer.subscribe("create", "message");
        latch.await(60, TimeUnit.SECONDS);

        messageConsumer.shutdownGracefully();
    }
}
