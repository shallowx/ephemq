package org.meteor.example.consumer;

import org.meteor.client.consumer.Consumer;
import org.meteor.client.consumer.ConsumerConfig;
import org.meteor.client.internal.ClientConfig;
import org.meteor.remote.util.ByteBufUtils;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ConsumerExample {

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
            String msg = ByteBufUtils.buf2String(message, message.readableBytes());
            System.out.printf("messageId=%s topic=%s queue=%s message=%s%n", messageId, topic, queue, msg);
        });
        consumer.start();
        this.consumer = consumer;
    }

    public void subscribe() throws Exception {
        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.subscribe("#test#default#topic", symbol);
        }
        new CountDownLatch(1).await();
    }

    public void deSubscribe() throws Exception {
        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.deSubscribe("#test#default#topic", symbol);
        }
        new CountDownLatch(1).await();
    }

    public void clean() {
        consumer.clear("detest#default#topic")
    }
}
