package org.leopard.example.quickstart;

import org.junit.jupiter.api.Test;
import org.leopard.client.ClientConfig;
import org.leopard.client.Message;
import org.leopard.client.consumer.*;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Subscription;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
