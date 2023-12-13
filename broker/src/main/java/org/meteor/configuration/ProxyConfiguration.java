package org.meteor.configuration;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class ProxyConfiguration {
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
    private final CommonConfiguration commonConfiguration;
    private final NetworkConfiguration networkConfiguration;
    private final ZookeeperConfiguration zookeeperConfiguration;

    private final SegmentConfiguration segmentConfiguration;

    public ProxyConfiguration(Properties prop, CommonConfiguration configuration, NetworkConfiguration networkConfiguration, ZookeeperConfiguration zookeeperConfiguration, SegmentConfiguration segmentConfiguration) {
        this.prop = prop;
        this.commonConfiguration = configuration;
        this.networkConfiguration = networkConfiguration;
        this.zookeeperConfiguration = zookeeperConfiguration;
        this.segmentConfiguration = segmentConfiguration;
    }

    public SegmentConfiguration getSegmentConfiguration() {
        return segmentConfiguration;
    }

    public ZookeeperConfiguration getZookeeperConfiguration() {
        return zookeeperConfiguration;
    }

    public CommonConfiguration getCommonConfiguration() {
        return commonConfiguration;
    }

    public NetworkConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    public int getProxyLeaderSyncPeriodMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_PERIOD_MS, 60000));
    }

    public int getProxyLeaderSyncSemaphore() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_SEMAPHORE, 100));
    }

    public int getProxyLeaderSyncUpstreamTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MS, 1900));
    }
    public int getProxyChannelConnectionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_CHANNEL_CONNECTION_TIMEOUT_MS, 3000));
    }
    public int getProxyResumeTaskScheduleDelayMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_RESUME_TASK_SCHEDULE_DELAY_MS, 3000));
    }

    public int getProxySyncCheckIntervalMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_SYNC_CHECK_INTERVAL_MS, 5000));
    }

    public int getProxyTopicChangeDelayMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_TOPIC_CHANGE_DELAY_MS, 15000));
    }

    public String getProxyUpstreamServers() {
        return TypeTransformUtils.object2String(prop.getOrDefault(PROXY_UPSTREAM_SERVERS, "127.0.0.1:19527"));
    }

    public int getProxyHeavyLoadSubscriberThreshold() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD, 200000));
    }

    public int getProxyClientWorkerThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_CLIENT_WORKER_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }

    public int getProxyClientPoolSize() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_CLIENT_POOL_SIZE, 3));
    }

    public int getProxyLeaderSyncInitialDelayMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_INITIAL_DELAY_MS, 60000));
    }
}
