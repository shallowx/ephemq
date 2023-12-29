package org.meteor.example.consumer;

import org.meteor.client.consumer.Consumer;
import org.meteor.client.consumer.ConsumerConfig;
import org.meteor.client.internal.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.util.ByteBufUtil;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ConsumerExample {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerExample.class);
    private static final String EXAMPLE_TOPIC = "example-topic";
    private static final String EXAMPLE_TOPIC_QUEUE = "example-topic-queue";

    public static void main(String[] args) throws Exception {
        ConsumerExample example = new ConsumerExample();
        example.subscribe();
        example.cancelSubscribe();
        example.clear();
    }

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

        Consumer consumer = new Consumer("default-consumer", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            if (logger.isInfoEnabled()) {
                logger.info("messageId={} topic={} queue={} message={} \n", messageId, topic, queue, msg);
            }
        });
        consumer.start();
        this.consumer = consumer;
    }

    public void subscribe() throws Exception {
        String[] symbols = new String[]{EXAMPLE_TOPIC_QUEUE};
        for (String symbol : symbols) {
            consumer.subscribe(EXAMPLE_TOPIC, symbol);
        }
        new CountDownLatch(1).await();
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
