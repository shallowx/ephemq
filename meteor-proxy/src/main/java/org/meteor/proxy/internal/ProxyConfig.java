package org.meteor.proxy.internal;

import io.netty.util.NettyRuntime;
import org.meteor.config.CommonConfig;
import org.meteor.config.ZookeeperConfig;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2String;

public class ProxyConfig {
    private static final String PROXY_UPSTREAM_SERVERS = "proxy.upstream.servers";
    private static final String PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD = "proxy.heavy.load.subscriber.threshold";
    private static final String PROXY_CLIENT_WORKER_THREAD_LIMIT = "proxy.client.worker.thread.limit";
    private static final String PROXY_CLIENT_POOL_SIZE = "proxy.client.pool.size";
    private static final String PROXY_LEDGER_SYNC_INITIAL_DELAY_MILLISECONDS = "proxy.ledger.sync.initial.delay.milliseconds";
    private static final String PROXY_LEDGER_SYNC_PERIOD_MILLISECONDS = "proxy.ledger.sync.period.milliseconds";
    private static final String PROXY_LEDGER_SYNC_SEMAPHORE = "proxy.ledger.sync.semaphore";
    private static final String PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MILLISECONDS = "proxy.ledger.sync.upstream.timeout.milliseconds";
    private static final String PROXY_CHANNEL_CONNECTION_TIMEOUT_MILLISECONDS = "proxy.channel.connection.timeout.milliseconds";
    private static final String PROXY_RESUME_TASK_SCHEDULE_DELAY_MILLISECONDS = "proxy.resume.task.schedule.delay.milliseconds";
    private static final String PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS = "proxy.sync.check.interval.milliseconds";
    private static final String PROXY_TOPIC_CHANGE_DELAY_MILLISECONDS = "proxy.topic.change.delay.milliseconds";
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

    public int getProxyLeaderSyncPeriodMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_PERIOD_MILLISECONDS, 60000));
    }

    public int getProxyLeaderSyncSemaphore() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_SEMAPHORE, 100));
    }

    public int getProxyLeaderSyncUpstreamTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MILLISECONDS, 1900));
    }

    public int getProxyChannelConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_CHANNEL_CONNECTION_TIMEOUT_MILLISECONDS, 3000));
    }

    public int getProxyResumeTaskScheduleDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_RESUME_TASK_SCHEDULE_DELAY_MILLISECONDS, 3000));
    }

    public int getProxySyncCheckIntervalMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_SYNC_CHECK_INTERVAL_MILLISECONDS, 5000));
    }

    public int getProxyTopicChangeDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_TOPIC_CHANGE_DELAY_MILLISECONDS, 15000));
    }

    public String getProxyUpstreamServers() {
        return object2String(prop.getOrDefault(PROXY_UPSTREAM_SERVERS, "127.0.0.1:9527"));
    }

    public int getProxyHeavyLoadSubscriberThreshold() {
        return object2Int(prop.getOrDefault(PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD, 200000));
    }

    public int getProxyClientWorkerThreadLimit() {
        return object2Int(prop.getOrDefault(PROXY_CLIENT_WORKER_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    public int getProxyClientPoolSize() {
        return object2Int(prop.getOrDefault(PROXY_CLIENT_POOL_SIZE, 3));
    }

    public int getProxyLeaderSyncInitialDelayMilliseconds() {
        return object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_INITIAL_DELAY_MILLISECONDS, 60000));
    }
}
