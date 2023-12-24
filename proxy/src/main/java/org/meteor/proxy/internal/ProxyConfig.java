package org.meteor.proxy.internal;

import io.netty.util.NettyRuntime;
import org.meteor.common.util.TypeTransformUtil;
import org.meteor.config.CommonConfig;
import org.meteor.config.ZookeeperConfig;

import java.util.Properties;

public class ProxyConfig {
    private static final String PROXY_UPSTREAM_SERVERS = "proxy.upstream.servers";
    private static final String PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD = "proxy.heavy.load.subscriber.threshold";
    private static final String PROXY_CLIENT_WORKER_THREAD_LIMIT = "proxy.client.worker.thread.limit";
    private static final String PROXY_CLIENT_POOL_SIZE = "proxy.client.pool.size";
    private static final String PROXY_LEDGER_SYNC_INITIAL_DELAY_MS = "proxy.ledger.sync.initial.delay.ms";
    private static final String PROXY_LEDGER_SYNC_PERIOD_MS = "proxy.ledger.sync.period.ms";
    private static final String PROXY_LEDGER_SYNC_SEMAPHORE = "proxy.ledger.sync.semaphore";
    private static final String PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MS = "proxy.ledger.sync.upstream.timeout.ms";
    private static final String PROXY_CHANNEL_CONNECTION_TIMEOUT_MS = "proxy.channel.connection.timeout.ms";
    private static final String PROXY_RESUME_TASK_SCHEDULE_DELAY_MS = "proxy.resume.task.schedule.delay.ms";
    private static final String PROXY_SYNC_CHECK_INTERVAL_MS = "proxy.sync.check.interval.ms";
    private static final String PROXY_TOPIC_CHANGE_DELAY_MS = "proxy.topic.change.delay.ms";

    private final Properties prop;
    private final CommonConfig commonConfiguration;
    private final ZookeeperConfig zookeeperConfiguration;

    public ProxyConfig(Properties prop, CommonConfig configuration, ZookeeperConfig zookeeperConfiguration) {
        this.prop = prop;
        this.commonConfiguration = configuration;
        this.zookeeperConfiguration = zookeeperConfiguration;
    }

    public ZookeeperConfig getZookeeperConfiguration() {
        return zookeeperConfiguration;
    }

    public CommonConfig getCommonConfiguration() {
        return commonConfiguration;
    }

    public int getProxyLeaderSyncPeriodMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_PERIOD_MS, 60000));
    }

    public int getProxyLeaderSyncSemaphore() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_SEMAPHORE, 100));
    }

    public int getProxyLeaderSyncUpstreamTimeoutMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MS, 1900));
    }
    public int getProxyChannelConnectionTimeoutMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_CHANNEL_CONNECTION_TIMEOUT_MS, 3000));
    }
    public int getProxyResumeTaskScheduleDelayMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_RESUME_TASK_SCHEDULE_DELAY_MS, 3000));
    }

    public int getProxySyncCheckIntervalMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_SYNC_CHECK_INTERVAL_MS, 5000));
    }

    public int getProxyTopicChangeDelayMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_TOPIC_CHANGE_DELAY_MS, 15000));
    }

    public String getProxyUpstreamServers() {
        return TypeTransformUtil.object2String(prop.getOrDefault(PROXY_UPSTREAM_SERVERS, "127.0.0.1:9527"));
    }

    public int getProxyHeavyLoadSubscriberThreshold() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD, 200000));
    }

    public int getProxyClientWorkerThreadLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_CLIENT_WORKER_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    public int getProxyClientPoolSize() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_CLIENT_POOL_SIZE, 3));
    }

    public int getProxyLeaderSyncInitialDelayMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_INITIAL_DELAY_MS, 60000));
    }
}
