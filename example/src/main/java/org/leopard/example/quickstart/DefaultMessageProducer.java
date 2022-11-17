package org.leopard.example.quickstart;

import org.junit.jupiter.api.Test;
import org.leopard.client.ClientConfig;
import org.leopard.client.Extras;
import org.leopard.client.Message;
import org.leopard.client.producer.MessageProducer;
import org.leopard.example.metadata.TopicMetadata;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.client.producer.MessagePreInterceptor;
import org.leopard.client.producer.Producer;
import org.leopard.client.producer.ProducerConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("all")
public class DefaultMessageProducer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadata.class);

    @Test
    public void sendAsync() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("async-producer", producerConfig);
        producer.start();

        Message message = new Message("create", "message", "message-test-send-async".getBytes(UTF_8), new Extras());
        MessagePreInterceptor filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (null == cause) {
                logger.warn("Send result - {}", sendResult);
            } else {
                logger.error(cause.getMessage(), cause);
            }
            latch.countDown();
        });

        latch.await();
        producer.shutdownGracefully();
    }
}
