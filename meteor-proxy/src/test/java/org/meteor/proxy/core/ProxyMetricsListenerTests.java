package org.meteor.proxy.core;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.junit.Assert;
import org.junit.Test;
import org.meteor.client.core.Client;
import org.meteor.config.CommonConfig;
import org.meteor.config.MetricsConfig;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.APIListener;
import org.meteor.listener.MetricsListener;
import org.meteor.zookeeper.ClusterManager;
import org.meteor.support.Connection;
import org.meteor.support.Manager;
import org.meteor.zookeeper.TopicHandleSupport;

import java.util.List;
import java.util.Properties;

public class ProxyMetricsListenerTests {

    @Test
    public void testProxyMetricsListener() {
        Properties prop = new Properties();
        prop.put("proxy.upstream.servers", "127.0.0.1:10000");
        CommonConfig commonConfiguration = new CommonConfig(prop);
        MetricsConfig metricsConfig = new MetricsConfig(prop);
        Manager manager = new Manager() {
            @Override
            public void start() throws Exception {

            }

            @Override
            public void shutdown() throws Exception {

            }

            @Override
            public TopicHandleSupport getTopicHandleSupport() {
                return null;
            }

            @Override
            public ClusterManager getClusterManager() {
                return null;
            }

            @Override
            public LogHandler getLogHandler() {
                return null;
            }

            @Override
            public Connection getConnection() {
                return null;
            }

            @Override
            public void addMetricsListener(MetricsListener listener) {

            }

            @Override
            public List<APIListener> getAPIListeners() {
                return List.of();
            }

            @Override
            public EventExecutorGroup getCommandHandleEventExecutorGroup() {
                return null;
            }

            @Override
            public EventExecutorGroup getMessageStorageEventExecutorGroup() {
                return null;
            }

            @Override
            public EventExecutorGroup getMessageDispatchEventExecutorGroup() {
                return null;
            }

            @Override
            public EventExecutorGroup getAuxEventExecutorGroup() {
                return null;
            }

            @Override
            public List<EventExecutor> getAuxEventExecutors() {
                return List.of();
            }

            @Override
            public Client getInternalClient() {
                return null;
            }
        };
        ProxyMetricsListener proxyMetricsListener = new ProxyMetricsListener(prop, commonConfiguration, metricsConfig, manager);
        Assert.assertNotNull(proxyMetricsListener);
    }
}
