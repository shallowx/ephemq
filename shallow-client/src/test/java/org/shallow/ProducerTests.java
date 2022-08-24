package org.shallow;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.producer.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.shallow.util.ObjectUtil.isNull;

@SuppressWarnings("all")
public class ProducerTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTests.class);

    private static ClientConfig clientConfig;
    private static Client client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9100"));

        client = new Client("producer-client", clientConfig);
        client.start();
    }

    @Test
    public void testSend() throws Exception {
        MessageProducer producer = new MessageProducer("send-producer", client, new ProducerConfig());
        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);
        MessageFilter filter = sendMessage -> sendMessage;

        SendResult result = producer.send(message, filter);
        Assert.assertNotNull(result);
        logger.info("send result - {}", result);
    }

    @Test
    public void testSendOneway() {
        MessageProducer producer = new MessageProducer("oneway-producer", client, new ProducerConfig());
        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);

        producer.sendOneway(message, new MessageFilter() {
            @Override
            public Message filter(Message message) {
                return message;
            }
        });
    }

    @Test
    public void testSendAsync() throws InterruptedException {
        MessageProducer producer = new MessageProducer("async-producer", client, new ProducerConfig());
        Message message = new Message("create", "message", "message".getBytes(UTF_8), null);
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
    }

    @AfterClass
    public static void afterClass() throws Exception {
        client.shutdownGracefully();
    }
}
