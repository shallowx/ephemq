package org.meteor.example.consumer;

import org.meteor.client.consumer.Consumer;
import org.meteor.client.consumer.ConsumerConfig;
import org.meteor.client.internal.ClientConfig;
import org.meteor.remote.util.ByteBufUtils;
import java.util.ArrayList;
public class ConsumerExample {

    public void subscribe() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new Consumer("default", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtils.buf2String(message, message.readableBytes());
            System.out.printf("messageId=%s topic=%s queue=%s message=%s%n", messageId, topic, queue, msg);
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.subscribe("#test#default", symbol);
        }
    }


    public void deSubscribe() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });
        clientConfig.setConnectionPoolCapacity(2);

        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setClientConfig(clientConfig);

        Consumer consumer = new Consumer("default", consumerConfig, (topic, queue, messageId, message, extras) -> {
            String msg = ByteBufUtils.buf2String(message, message.readableBytes());
            System.out.printf("messageId=%s topic=%s queue=%s message=%s%n", messageId, topic, queue, msg);
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.deSubscribe("#test#default", symbol);
        }
    }
}
