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

/**
 * The ProducerExample class demonstrates various ways to send messages using a producer.
 * It includes methods to send messages synchronously, asynchronously, and using one-way sends.
 */
public class ProducerExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerExample.class);
    /**
     * The topic name for the message producer to which messages will be sent.
     * Used to specify the particular topic in a message broker where the producer
     * should send the messages.
     */
    private static final String EXAMPLE_TOPIC = "example-topic";
    /**
     * A constant that represents the name of the queue for the example topic.
     * This value is used to specify the destination queue in message production.
     */
    private static final String EXAMPLE_TOPIC_QUEUE = "example-topic-queue";
    /**
     * The Producer instance used for sending messages to a specific topic and queue.
     * It provides methods to start the producer, send messages synchronously and asynchronously,
     * send messages with a timeout, send one-way messages, and close the producer.
     */
    private final Producer producer;

    /**
     * The main entry point of the application, which demonstrates sending messages using different methods.
     *
     * @param args Command-line arguments passed to the application.
     * @throws Exception if any error occurs during the sending of messages.
     */
    public static void main(String[] args) throws Exception {
        ProducerExample example = new ProducerExample();
        example.send();
        example.sendAsync();
        example.sendOneway();
    }

    /**
     *
     */
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

    /**
     * Sends messages indefinitely to a specified topic and queue in a non-blocking manner using virtual threads.
     * Utilizes a CountDownLatch to coordinate the sending process and ensure closure of the producer.
     *
     * @throws Exception if an error occurs during message sending or thread execution
     */
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

    /**
     * Sends a message to a predefined topic and queue. This method creates a map of extra properties,
     * generates a unique message ID, converts it to a ByteBuf, and sends it using the producer instance.
     */
    public void send() {
        Map<String, String> extras = new HashMap<>();
        extras.put("key", "v");
        producer.send(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE, ByteBufUtil.string2Buf(UUID.randomUUID().toString()), extras);
    }

    /**
     * Sends a message to a specified topic and queue with a timeout.
     * <p>
     * The method creates a message with a unique identifier, adds extra properties,
     * and sends it to a specified topic and queue using the producer's send method.
     * If the INFO logging level is enabled, it logs the message identifier.
     */
    public void sendWithTimeout() {
        Map<String, String> extras = new HashMap<>();
        extras.put("key", "v");
        MessageId messageId = producer.send(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE, ByteBufUtil.string2Buf(UUID.randomUUID().toString()), extras, 3000L);
        if (logger.isInfoEnabled()) {
            logger.info("messageId:{}", messageId);
        }
    }

    /**
     * Sends a message asynchronously to a predefined topic and queue.
     * The message payload is generated as a random UUID converted to a ByteBuf.
     * Upon completion, the provided `AsyncSendCallback` handles the response or errors.
     * Utilizes the `sendAsync` method of the producer instance with an empty extras map.
     */
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
