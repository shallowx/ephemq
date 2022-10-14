package org.shallow.namespace;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.shallow.internal.config.BrokerConfig;
import java.util.function.Supplier;

public class ZookeeperClient extends AbstractServerHolder<CuratorFramework> {

    private final BrokerConfig config;

    public ZookeeperClient(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public CuratorFramework createServer(String clusterName, Supplier<CuratorFramework> supplier) throws Exception {
        String uri = config.getUri();

        if (uri == null) {
            throw new IllegalArgumentException("Zookeeper address cannot be empty");
        }

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(uri)
                .sessionTimeoutMs(config.getSessionTimeoutMs())
                .connectionTimeoutMs(config.getConnectionTimeoutMs())
                .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                .namespace(clusterName)
                .retryPolicy(new ExponentialBackoffRetry(config.getConnectionRetrySleepMs(), config.getConnectionMaxRetries()))
                .build();

        client.start();
        return client;
    }
}
