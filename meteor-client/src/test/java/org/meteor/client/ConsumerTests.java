package org.meteor.client;

import org.junit.Test;
import org.meteor.client.consumer.ConsumerConfig;
import org.meteor.client.consumer.Consumer;
import org.meteor.client.internal.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.util.ByteBufUtil;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConsumerTests {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ConsumerTests.class);
    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testSubscribeOfReset() throws Exception {
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
            String msg = ByteBufUtil.buf2String(message, message.readableBytes());
            logger.info("messageId[{}] topic[{}] queue[{}] message[{}]", messageId, topic, queue, msg);
        });
        consumer.start();

        String[] symbols = new String[]{"test-queue"};
        for (String symbol : symbols) {
            consumer.subscribe("#test#default", symbol);
        }

        // the duration setting only for testing
        new CountDownLatch(1).await(10, TimeUnit.MINUTES);
        consumer.close();
    }
}
