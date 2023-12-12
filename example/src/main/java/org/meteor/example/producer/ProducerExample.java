package org.meteor.example.producer;

import io.netty.buffer.ByteBuf;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.producer.Producer;
import org.meteor.client.producer.ProducerConfig;
import org.meteor.common.Extras;
import org.meteor.remote.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerExample {
    public void sendOneway() throws Exception {
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
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendOneway("#test#default", symbol, message, new Extras());
                    } catch (Exception ignored) {}
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable ignored) {}
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }
}
