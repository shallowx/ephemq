package org.ephemq.proxy.core;

import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.ephemq.common.message.TopicConfig;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.support.DefaultMeteorManager;
import org.ephemq.support.Manager;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProxyLogTests {

    @Test
    public void testCancelSyncAndCloseIfNotSubscribe() throws ExecutionException, InterruptedException {
        Properties prop = new Properties();
        ProxyServerConfig proxyServerConfig = new ProxyServerConfig(prop);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        Manager manager = new DefaultMeteorManager(proxyServerConfig);
        TopicConfig topicConfig = new TopicConfig();

        ProxyLog proxyLog = new ProxyLog(proxyServerConfig, topicPartition, 1, 1, manager, topicConfig);
        Promise<Boolean> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        proxyLog.cancelSyncAndCloseIfNotSubscribe(promise);
        Assert.assertFalse(promise.isSuccess());
        Assert.assertTrue(promise.get());
    }
}
