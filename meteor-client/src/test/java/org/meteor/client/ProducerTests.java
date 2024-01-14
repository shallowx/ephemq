package org.meteor.client;

import io.netty.buffer.ByteBuf;
import org.junit.Test;
import org.meteor.client.producer.DefaultProducer;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.producer.Producer;
import org.meteor.client.producer.ProducerConfig;
import org.meteor.common.message.MessageId;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.util.ByteBufUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProducerTests {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProducerTests.class);

    @Test
    public void testContinueSend() throws Exception {
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
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new HashMap<>());
                        if (logger.isErrorEnabled()) {
                            logger.info("MessageId:[{}]", messageId);
                        }
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                }

                try {
                    // the duration setting only for testing
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error(t.getMessage(), t);
                    }
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
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendAsync("#test#default", symbol, message, new HashMap<>(), (messageId, t) -> {
                            if (t != null) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("Call back error:{}", t);
                                }
                            } else {
                                if (logger.isInfoEnabled()) {
                                    logger.info("MessageId:[{}]", messageId);
                                }
                            }
                        });
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
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
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        producer.sendOneway("#test#default", symbol, message, new HashMap<>());
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    // the duration setting only for testing
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }

    @Test
    public void testSendWithTimeout() throws InterruptedException {
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
                Producer producer = new DefaultProducer("default", producerConfig);
                producer.start();

                String[] symbols = new String[]{"test-queue"};
                for (int j = 0; j < Integer.MAX_VALUE; j++) {
                    String symbol = symbols[j % symbols.length];
                    ByteBuf message = ByteBufUtil.string2Buf(UUID.randomUUID().toString());
                    try {
                        MessageId messageId = producer.send("#test#default", symbol, message, new HashMap<>(), 3000L);
                        if (logger.isInfoEnabled()) {
                            logger.info("messageId:{}", messageId);
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }

                try {
                    // the duration setting only for testing
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Throwable t) {
                    if (logger.isInfoEnabled()) {
                        logger.info("error:{}", t);
                    }
                }
                producer.close();
                continueSendLatch.countDown();
            }).start();
        }
        continueSendLatch.await();
    }
}
