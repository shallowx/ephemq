package org.meteor.example.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.meteor.client.consumer.Consumer;
import org.meteor.client.consumer.ConsumerConfig;
import org.meteor.client.consumer.DefaultConsumer;
import org.meteor.client.core.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.util.ByteBufUtil;

public class ConsumerExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerExample.class);
    private static final String EXAMPLE_TOPIC = "example-topic";
    private static final String EXAMPLE_TOPIC_QUEUE = "example-topic-queue";
    private final Consumer consumer;

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

    public static void main(String[] args) throws Exception {
        ConsumerExample example = new ConsumerExample();
        example.subscribe();
        example.cancelSubscribe();
        example.clear();
    }

    public void subscribe() throws Exception {
        String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
        for (String symbol : symbols) {
            consumer.subscribe(EXAMPLE_TOPIC, symbol);
        }
        new CountDownLatch(1).await();
    }

    public void subscribeShip() {
        Map<String, String> ships = new HashMap<>();
        ships.put(EXAMPLE_TOPIC, EXAMPLE_TOPIC_QUEUE);
        consumer.subscribe(ships);
    }

    public void cancelSubscribe() throws Exception {
        String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
        for (String symbol : symbols) {
            consumer.cancelSubscribe(EXAMPLE_TOPIC, symbol);
        }
        new CountDownLatch(1).await();
    }

    public void clear() {
        consumer.clear(EXAMPLE_TOPIC);
    }
}
