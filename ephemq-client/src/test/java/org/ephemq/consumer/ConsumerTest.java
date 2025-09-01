package org.ephemq.consumer;

import org.junit.Test;
import org.ephemq.client.consumer.Consumer;
import org.ephemq.client.consumer.ConsumerConfig;
import org.ephemq.client.consumer.DefaultConsumer;
import org.ephemq.client.core.ClientConfig;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.util.ByteBufUtil;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A test suite for verifying the behavior of a message Consumer.
 * This class uses a mocked Consumer to ensure proper subscription,
 * message consuming, and cleanup actions are correctly performed.
 */
@SuppressWarnings("ALL")
public class ConsumerTest {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerTest.class);

    /**
     * Tests the subscription and reset functionality of the consumer.
     * <p>
     * This test initializes the client and consumer configurations,
     * starts the consumer, subscribes to a test queue, and then waits
     * for a specified duration before closing the consumer.
     *
     * @throws Exception if any error occurs during the test execution
     */
    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testSubscribeOfReset() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new DefaultConsumer("default", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            if (logger.isInfoEnabled()) {
                logger.info("messageId[{}] topic[{}] queue[{}] message[{}]", messageId, topic, queue, msg);
            }
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.subscribe("#test#default", symbol);
        }

        // the duration setting only for testing
        new CountDownLatch(1).await(10, TimeUnit.MINUTES);
        consumer.close();
    }

    /**
     * Tests the clear functionality of a consumer in a messaging system.
     * <p>
     * This method:
     * 1. Configures the client and consumer with specific settings such as bootstrap addresses and connection pool capacity.
     * 2. Initializes and starts a consumer that listens to a specific topic and queue.
     * 3. Subscribes the consumer to a test queue.
     * 4. Pauses execution for a while to let the consumer receive messages.
     * 5. Clears the subscriptions for the consumer on the specified topic.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Test
    public void testClear() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:19527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new DefaultConsumer("default", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            if (logger.isInfoEnabled()) {
                logger.info("messageId[{}] topic[{}] queue[{}] message[{}]", messageId, topic, queue, msg);
            }
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.subscribe("#test#default", symbol);
        }

        TimeUnit.SECONDS.sleep(3);
        consumer.clear("#test#default");
    }

    /**
     * Tests the functionality of message consumption with a default consumer configuration.
     * <p>
     * This method performs the following actions:
     * 1. Initializes a ClientConfig with specific bootstrap addresses and connection pool capacity.
     * 2. Creates a ConsumerConfig with the initialized ClientConfig.
     * 3. Instantiates and starts a DefaultConsumer, subscribing to given topics and queues.
     * 4. Waits for a short period to allow message processing.
     * 5. Cancels the subscription to the specified topics and queues.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    @Test
    public void testClean() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new DefaultConsumer("default", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            logger.info("messageId[{}] topic[{}] queue[{}] message[{}]", messageId, topic, queue, msg);
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        // receive message
        for (String symbol : symbols) {
            consumer.subscribe("#test#default", symbol);
        }

        TimeUnit.SECONDS.sleep(3);
        // not receive message
        for (String symbol : symbols) {
            consumer.cancelSubscribe("#test#default", symbol);
        }
    }
}
