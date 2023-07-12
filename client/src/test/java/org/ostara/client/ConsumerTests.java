package org.ostara.client;

import org.junit.Test;
import org.ostara.client.consumer.Consumer;
import org.ostara.client.consumer.ConsumerConfig;
import org.ostara.client.internal.ClientConfig;
import org.ostara.remote.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerTests {
    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testAttachOfReset() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:8888");
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

        String[] symbols = new String[]{"BTC-USDT"};
        for (String symbol : symbols) {
            consumer.attach("#test#default", symbol);
        }

        new CountDownLatch(1).await(10, TimeUnit.MINUTES);
        consumer.close();
    }
}
