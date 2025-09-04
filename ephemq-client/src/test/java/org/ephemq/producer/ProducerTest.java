package org.ephemq.producer;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.ephemq.client.core.ClientConfig;
import org.ephemq.client.producer.DefaultProducer;
import org.ephemq.client.producer.Producer;
import org.ephemq.client.producer.ProducerConfig;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.MessageId;
import org.ephemq.remote.util.ByteBufUtil;

/**
 * This class contains unit tests to verify the functionality of a producer in a messaging system.
 * The tests cover various sending methods including synchronous, asynchronous, one-way, and
 * sending with a timeout.
 */
public class ProducerTest {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTest.class);

    /**
     * Tests the continuous sending of messages by simulating a producer that sends messages
     * to a specified queue in an infinite loop. The method configures the client and producer,
     * starts a virtual thread to send messages, and logs the message IDs or errors encountered.
     *
     * @throws Exception if any error occurs during the message sending process
     */
    @Test
    public void testContinueSend() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            Thread.ofVirtual().unstarted(() -> {
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new HashMap<>());
                        if (logger.isErrorEnabled()) {
                            logger.info("MessageId:[{}]", messageId);
                        }
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }

                try {
                    // the duration setting only for testing and demonstration purposes
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error(t.getMessage(), t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    /**
     * Tests asynchronous message sending function of a producer with a given configuration.
     *
     * @throws Exception if any error occurs during the test execution.
     *                   <p>
     *                   This method performs the following steps:
     *                   1. Creates and configures a ClientConfig instance with a specific bootstrap address and connection pool capacity.
     *                   2. Configures a ProducerConfig instance with the previously created ClientConfig.
     *                   3. Initializes a CountDownLatch to synchronize operations.
     *                   4. Starts a virtual thread to create and start a producer, sending messages asynchronously in a loop.
     *                   5. Each message send operation provides a callback to handle success or error cases.
     *                   6. Virtual thread sleeps for a short period and then closes the producer.
     *                   7. Waits for all virtual threads to complete before finishing the test.
     */
    @Test
    public void testContinueSendAsync() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            Thread.ofVirtual().start(() -> {
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendAsync("#test#default", symbol, message, new HashMap<>(), (messageId, t) -> {
                            if (t != null) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Call back error:{}", t);
                                }
                            } else {
                                if (logger.isInfoEnabled()) {
                                    logger.info("MessageId:[{}]", messageId);
                                }
                            }
                        });
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            });
        }
        continueSendLatch.await();
    }

    /**
     * Tests the functionality of continuously sending one-way messages
     * using a {@link Producer} configured with a {@link ProducerConfig}.
     *
     * @throws Exception if an error occurs during message production or
     *                   while waiting for message sending to complete.
     */
    @Test
    public void testContinueSendOneway() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            Thread.ofVirtual().start(() -> {
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendOneway("#test#default", symbol, message, new HashMap<>());
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    // the duration setting only for testing
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    /**
     * Tests the functionality of sending messages with a timeout using the Producer.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    @Test
    public void testSendWithTimeout() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            Thread.ofVirtual().start(() -> {
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new HashMap<>(), 3000L);
                        if (logger.isInfoEnabled()) {
                            logger.info("messageId:{}", messageId);
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    // the duration setting only for testing
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }
}
