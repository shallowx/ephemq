package org.shallow;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.producer.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.util.ObjectUtil.isNull;

@SuppressWarnings("all")
public class ProducerTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTests.class);

    private static ClientConfig clientConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));
    }

    @Test
    public void testSend() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("send-producer", producerConfig);
        producer.start();

        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);
        MessageFilter filter = sendMessage -> sendMessage;

        SendResult result = producer.send(message, filter);
        Assert.assertNotNull(result);
        logger.info("send result - {}", result);

        producer.shutdownGracefully();
    }

    @Test
    public void testSendOneway() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("oneway-producer", producerConfig);
        producer.start();

        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);

        producer.sendOneway(message, new MessageFilter() {
            @Override
            public Message filter(Message message) {
                return message;
            }
        });

        producer.shutdownGracefully();
    }

    @Test
    public void testSendAsync() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("async-producer", producerConfig);
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

    @Test
    public void testSendWithExtras() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("async-producer", producerConfig);
        producer.start();

        Map<String, String> extras = new HashMap<>();
        extras.put("extras0", "send-message-filter0");
        extras.put("extras1", "send-message-filter1");

        Message message = new Message("create", "message", "message-test-send-async".getBytes(UTF_8), new Message.Extras(extras));
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

    @AfterClass
    public static void afterClass() throws Exception {
    }
}
