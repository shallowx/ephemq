package org.ephemq.example.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.ephemq.client.consumer.Consumer;
import org.ephemq.client.consumer.ConsumerConfig;
import org.ephemq.client.consumer.DefaultConsumer;
import org.ephemq.client.core.ClientConfig;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.remote.util.ByteBufUtil;

/**
 * The ConsumerExample class demonstrates a simple usage of a message consumer.
 * It subscribes to a topic, processes incoming messages, and provides methods
 * to subscribe, cancel subscription, and clear subscriptions.
 */
public class ConsumerExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerExample.class);
    /**
     *
     */
    private static final String EXAMPLE_TOPIC = "example-topic";
    /**
     * The queue name for the example topic subscription.
     * Used to subscribe and manage messages from the "example-topic-queue".
     */
    private static final String EXAMPLE_TOPIC_QUEUE = "example-topic-queue";
    /**
     * The consumer handles subscribing to topics, processing incoming messages, and managing subscriptions.
     * It uses a provided topic, queue, and message handler to operate.
     */
    private final Consumer consumer;

    /**
     * The main method to demonstrate the functionality of the ConsumerExample class.
     * It performs subscription to a topic, cancels the subscription, and clears all subscriptions.
     *
     * @param args Command line arguments.
     * @throws Exception if there is any issue during subscription or cancellation.
     */
    public static void main(String[] args) throws Exception {
        ConsumerExample example = new ConsumerExample();
        example.subscribe();
        example.cancelSubscribe();
        example.clear();
    }

    /**
     * Constructor for the ConsumerExample class.
     * This sets up and starts a consumer configured to connect to a local address.
     * <p>
     * The method initializes the ClientConfig and sets the bootstrap addresses to connect
     * to either a broker or a proxy. It also configures the connection pool capacity.
     * <p>
     * It then creates a ConsumerConfig using the initialized client configuration and
     * instantiates the consumer with a message processing callback that logs the received
     * messages.
     */
    public ConsumerExample() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                // Supports connection to broker and proxy
                // if only broker is required, set the broker address
                // if proxy is required, set the proxy address,and 'proxy.upstream.servers' set the broker address
                add("127.0.0.1:9527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new DefaultConsumer("default-consumer", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            if (logger.isInfoEnabled()) {
                logger.info("messageId[{}] topic[{}] queue[{}] message[{}]", messageId, topic, queue, msg);
            }
        });
        this.consumer = consumer;
        consumer.start();
    }

    /**
     * Subscribes the consumer to specified message queues of a topic and waits indefinitely.
     *
     * @throws Exception if an error occurs during subscription or awaiting.
     */
    public void subscribe() throws Exception {
        String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
        for (String symbol : symbols) {
            consumer.subscribe(EXAMPLE_TOPIC, symbol);
        }
        new CountDownLatch(1).await();
    }

    /**
     * Subscribes the consumer to a predetermined topic and its queue.
     * This method sets up a subscription using predefined constants
     * for the example topic and its corresponding queue. It leverages
     * the underlying consumer's subscribe method to establish these
     * subscriptions.
     */
    public void subscribeShip() {
        Map<String, String> ships = new HashMap<>();
        ships.put(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE);
        consumer.subscribe(ships);
    }

    /**
     * Cancels the current subscription to a specific queue within a topic.
     * This method iterates over an array of queue names and cancels the
     * subscription for each queue under the specified topic.
     *
     * @throws Exception if awaiting termination is interrupted
     */
    public void cancelSubscribe() throws Exception {
        String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
        for (String symbol : symbols) {
            consumer.cancelSubscribe(EXAMPLE_TOPIC, symbol);
        }
        new CountDownLatch(1).await();
    }

    /**
     * Clears all subscriptions for the specified topic.
     * This method removes any existing subscriptions to the topic,
     * effectively stopping any further messages from being received for it.
     */
    public void clear() {
        consumer.clear(EXAMPLE_TOPIC);
    }
}
