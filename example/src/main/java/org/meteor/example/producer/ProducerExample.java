package org.meteor.example.producer;

import io.netty.buffer.ByteBuf;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.producer.Producer;
import org.meteor.client.producer.ProducerConfig;
import org.meteor.client.producer.SendCallback;
import org.meteor.common.message.Extras;
import org.meteor.common.message.MessageId;
import org.meteor.remote.util.ByteBufUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerExample {

    private final Producer producer;

    public ProducerExample() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>() {
            {
                add("127.0.0.1:9527");
            }
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        Producer producer = new Producer("default-producer", producerConfig);
        producer.start();
        this.producer = producer;
    }

    public void sendOneway() throws Exception {
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
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

    public void send() {
        Map<String, String> entries =  new HashMap<>();
        entries.put("key", "v");
        Extras extras = new Extras(entries);
        producer.send("#test#default", "test-topic", ByteBufUtil.string2Buf(UUID.randomUUID().toString()), extras);
    }

    public void sendAsync() {
        producer.sendAsync("#test#default", "test-topic", ByteBufUtil.string2Buf(UUID.randomUUID().toString()), new Extras(), new AsyncSendCallback());
    }

    static class AsyncSendCallback implements SendCallback {
        @Override
        public void onCompleted(MessageId messageId, Throwable t) {
            // what to do as needed
        }
    }
}
