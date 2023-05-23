package org.ostara.client.quickstart;

import io.netty.buffer.ByteBuf;
import org.checkerframework.checker.units.qual.C;
import org.junit.Test;
import org.ostara.client.ClientConfig;
import org.ostara.client.consumer.Consumer;
import org.ostara.client.consumer.ConsumerConfig;
import org.ostara.client.consumer.MessageListener;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientListener;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;
import org.ostara.remote.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerTest {
    @Test
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

        Consumer consumer = new Consumer("default", consumerConfig, new MessageListener() {
            @Override
            public void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Extras extras) {
                String msg = ByteBufUtils.buf2String(message, message.readableBytes());
                System.out.printf("messageId=%s topic=%s queue=%s message=%s%n", messageId, topic, queue, msg);
            }
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
