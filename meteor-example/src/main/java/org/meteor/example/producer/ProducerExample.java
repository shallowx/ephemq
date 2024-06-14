package org.meteor.example.producer;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.producer.DefaultProducer;
import org.meteor.client.producer.Producer;
import org.meteor.client.producer.ProducerConfig;
import org.meteor.client.producer.SendCallback;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.remote.util.ByteBufUtil;

public class ProducerExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerExample.class);
    private static final String EXAMPLE_TOPIC = "example-topic";
    private static final String EXAMPLE_TOPIC_QUEUE = "example-topic-queue";
    private final Producer producer;

    public static void main(String[] args) throws Exception {
        ProducerExample example = new ProducerExample();
        example.send();
        example.sendAsync();
        example.sendOneway();
    }

    public ProducerExample() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        Producer producer = new DefaultProducer("default-producer", producerConfig);
        producer.start();
        this.producer = producer;
    }

    public void sendOneway() throws Exception {
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            Thread.ofVirtual().start(() -> {
                String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendOneway(EXAMPLE_TOPIC, symbol, message, new HashMap<>());
                    } catch (Exception ignored) {
                    }
                }

                try {
                    // the duration setting only for testing and demonstration purposes
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable ignored) {
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    public void send() {
        Map<String, String> extras = new HashMap<>();
        extras.put("key", "v");
        producer.send(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE, ByteBufUtil.string2Buf(UUID.randomUUID().toString()), extras);
    }

    public void sendWithTimeout() {
        Map<String, String> extras = new HashMap<>();
        extras.put("key", "v");
        MessageId messageId = producer.send(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE, ByteBufUtil.string2Buf(UUID.randomUUID().toString()), extras, 3000L);
        if (logger.isInfoEnabled()) {
            logger.info("messageId:{}", messageId);
        }
    }

    public void sendAsync() {
        producer.sendAsync(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE, ByteBufUtil.string2Buf(UUID.randomUUID().toString()), new HashMap<>(), new AsyncSendCallback());
    }

    static class AsyncSendCallback implements SendCallback {
        @Override
        public void onCompleted(MessageId messageId, Throwable t) {
            // what to do as needed
            if (logger.isErrorEnabled()) {
                logger.info("message_id - {}", messageId);
            }
        }
    }
}
