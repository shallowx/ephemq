package org.ostara.example.quickstart;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.ostara.client.Extras;
import org.ostara.client.Message;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.producer.MessagePreInterceptor;
import org.ostara.client.producer.MessageProducer;
import org.ostara.client.producer.Producer;
import org.ostara.client.producer.ProducerConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.example.metadata.TopicMetadataWriterExample;

@SuppressWarnings("all")
public class DefaultMessageProducer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataWriterExample.class);

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
