package org.ostara.client;

import io.netty.buffer.ByteBuf;
import org.junit.Test;
import org.ostara.client.producer.Producer;
import org.ostara.client.producer.ProducerConfig;
import org.ostara.client.producer.SendCallback;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.util.ByteBufUtils;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTests.class);

    @Test
    public void testContinueSend() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new Extras());
                        logger.info("MessageId:[{}]", messageId);
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
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    @Test
    public void testContinueSendAsync() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendAsync("#test#default", symbol, message, new Extras(), (messageId, t) -> {
                            if (t != null) {
                                logger.error("Call back error:{}", t);
                            } else {
                                logger.info("MessageId:[{}]", messageId);
                            }
                        });
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    logger.info("error:{}", t);
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    @Test
    public void testContinueSendOneway() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapAddresses(new ArrayList<>(){
            {add("127.0.0.1:8888");}
        });

        clientConfig.setConnectionPoolCapacity(2);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setClientConfig(clientConfig);
        CountDownLatch continueSendLatch = new CountDownLatch(2);
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                Producer producer = new Producer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"BTC-USDT"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtils.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendOneway("#test#default", symbol, message, new Extras());
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t){
                    logger.info("error:{}", t);
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }
}
