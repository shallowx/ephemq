package org.leopard.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.leopard.client.internal.ClientConfig;
import org.leopard.client.producer.MessagePreInterceptor;
import org.leopard.client.producer.MessageProducer;
import org.leopard.client.producer.Producer;
import org.leopard.client.producer.ProducerConfig;
import org.leopard.client.producer.SendResult;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;

@SuppressWarnings("all")
public class ProducerTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTests.class);

    private static ClientConfig clientConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
    }

    @Test
    public void testSend() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("send-producer", producerConfig);
        producer.start();

        Message message = new Message("test", "message", "message".getBytes(UTF_8), null);
        MessagePreInterceptor filter = sendMessage -> sendMessage;

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

        Message message = new Message("test", "message", "message".getBytes(UTF_8), null);

        producer.sendOneway(message, new MessagePreInterceptor() {
            @Override
            public Message interceptor(Message message) {
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

        Message message = new Message("test", "message", "message-test-send-async".getBytes(UTF_8), new Extras());
        MessagePreInterceptor filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (null == cause) {
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
    public void testSendAsyncWithVersion() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("async-producer", producerConfig);
        producer.start();

        short messageVersion = 2;
        Message message = new Message("test", "message", messageVersion, "message-test-send-async".getBytes(UTF_8),
                new Extras());
        MessagePreInterceptor filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (null == cause) {
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

        Message message =
                new Message("test", "message", "message-test-send-async".getBytes(UTF_8), new Extras(extras));
        MessagePreInterceptor filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendAsync(message, filter, (sendResult, cause) -> {
            if (null == cause) {
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
    public void testContinueSend() throws Exception {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);

        Producer producer = new MessageProducer("async-producer", producerConfig);
        producer.start();

        MessagePreInterceptor filter = sendMessage -> sendMessage;

        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < Long.MAX_VALUE; i++) {
            Message message =
                    new Message("test", "message", ("message-test-send-async" + i).getBytes(UTF_8), new Extras());
            producer.sendAsync(message, filter, (sendResult, cause) -> {
                if (null == cause) {
                    if (logger.isInfoEnabled()) {
                        logger.info("send result - {}", sendResult);
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error(cause);
                    }
                }
            });

            TimeUnit.SECONDS.sleep(1);
        }
        latch.wait();
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }
}
