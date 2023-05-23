package org.ostara.client.quickstart;

import io.netty.buffer.ByteBuf;
import org.checkerframework.checker.units.qual.C;
import org.junit.Test;
import org.ostara.client.ClientConfig;
import org.ostara.client.producer.Producer;
import org.ostara.client.producer.ProducerConfig;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;
import org.ostara.remote.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerTest {

    @Test
    public void testContinueSend() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                Random random = new Random();
                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new Extras());
                        System.out.println(messageId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    t.printStackTrace();
                }
                producer.close();
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
    }
}
