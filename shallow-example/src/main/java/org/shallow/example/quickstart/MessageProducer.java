package org.shallow.example.quickstart;

import org.junit.jupiter.api.Test;
import org.shallow.ClientConfig;
import org.shallow.Message;
import org.shallow.example.metadata.TopicMetadata;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.producer.MessageFilter;
import org.shallow.producer.Producer;
import org.shallow.producer.ProducerConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.util.ObjectUtil.isNull;

@SuppressWarnings("all")
public class MessageProducer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadata.class);

    @Test
    public void sendAsync() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new org.shallow.producer.MessageProducer("async-producer", producerConfig);
        producer.start();

        Message message = new Message("create", "message", "message-test-send-async".getBytes(UTF_8), new Message.Extras());
        MessageFilter filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (isNull(cause)) {
                logger.warn("send result - {}", sendResult);
            } else {
                logger.error(cause);
            }
            latch.countDown();
        });

        latch.await();
        producer.shutdownGracefully();
    }
}
