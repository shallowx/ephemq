package org.meteor.core;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class CoreConfig {
    private static final String SERVER_ID = "server.id";
    private static final String CLUSTER_NAME = "server.cluster.name";
    private static final String ADVERTISED_ADDRESS = "server.advertised.address";
    private static final String ADVERTISED_PORT = "server.advertised.port";
    private static final String COMPATIBLE_PORT = "server.compatible.port";
    private static final String NETWORK_THREAD_COUNTS = "network.thread.counts";
    private static final String IO_THREAD_COUNTS = "io.thread.counts";
    private static final String ZOOKEEPER_URL = "zookeeper.url";
    private static final String ZOOKEEPER_CONNECTION_RETRY_SLEEP_MS = "zookeeper.connection.retry.sleep.ms";
    private static final String ZOOKEEPER_CONNECTION_RETRIES = "zookeeper.connection.retries";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    private static final String SEGMENT_ROLLING_SIZE = "segment.rolling.size";
    private static final String SEGMENT_RETAIN_COUNTS = "segment.retain.counts";
    private static final String SEGMENT_RETAIN_TIME = "segment.retain.time.ms";
    private static final String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    private static final String NETWORK_LOG_DEBUG_ENABLED = "network.log.debug.enabled";
    private static final String WRITE_BUFFER_WATER_MARK = "socket.write.buffer.high.watermark";
    private static final String NOTIFY_CLIENT_TIMEOUT_MS = "notify.client.timeout.ms";
    private static final String COMMAND_HANDLE_THREAD_COUNTS = "command.handler.thread.counts";
    private static final String MESSAGE_SYNC_THREAD_COUNTS = "message.sync.thread.counts";
    private static final String MESSAGE_STORAGE_THREAD_COUNTS = "message.storage.thread.counts";
    private static final String MESSAGE_DISPATCH_THREAD_COUNTS = "message.dispatch.thread.counts";
    private static final String DISPATCH_ENTRY_LOAD_LIMIT = "dispatch.entry.load.limit";
    private static final String DISPATCH_ENTRY_FOLLOW_LIMIT = "dispatch.entry.follow.limit";
    private static final String DISPATCH_ENTRY_PURSUE_LIMIT = "dispatch.entry.pursue.limit";
    private static final String DISPATCH_ENTRY_ALIGN_LIMIT = "dispatch.entry.align.limit";
    private static final String DISPATCH_ENTRY_PURSUE_TIMEOUT_MS = "dispatch.entry.pursue.timeout.ms";
    private static final String SHUTDOWN_MAX_WAIT_TIME_MS = "shutdown.max.wait.time.ms";
    private static final String METRICS_SAMPLE_COUNT = "metrics.sample.count";
    private static final String AUX_THREAD_COUNTS = "aux.thread.counts";
    private static final String PROXY_UPSTREAM_SERVERS = "proxy.upstream.servers";
    private static final String PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD = "proxy.heavy.load.subscriber.threshold";
    private static final String PROXY_CLIENT_WORKER_THREAD_COUNTS = "proxy.client.worker.thread.counts";
    private static final String PROXY_CLIENT_POOL_SIZE = "proxy.client.pool.size";
    private static final String PROXY_LEDGER_SYNC_INITIAL_DELAY_MS = "proxy.ledger.sync.initial.delay.ms";
    private static final String PROXY_LEDGER_SYNC_PERIOD_MS = "proxy.ledger.sync.period.ms";
    private static final String PROXY_LEDGER_SYNC_SEMAPHORE = "proxy.ledger.sync.semaphore";
    private static final String PROXY_LEDGER_SYNC_UPSTREAM_TIMEOUT_MS = "proxy.ledger.sync.upstream.timeout.ms";
    private static final String PROXY_CHANNEL_CONNECTION_TIMEOUT_MS = "proxy.channel.connection.timeout.ms";
    private static final String PROXY_RESUME_TASK_SCHEDULE_DELAY_MS = "proxy.resume.task.schedule.delay.ms";
    private static final String PROXY_SYNC_CHECK_INTERVAL_MS = "proxy.sync.check.interval.ms";
    private static final String PROXY_TOPIC_CHANGE_DELAY_MS = "proxy.topic.change.delay.ms";
    private static final String CHUNK_DISPATCH_ENTRY_LOAD_LIMIT = "chunk.dispatch.entry.load.limit";
    private static final String CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT = "chunk.dispatch.entry.follow.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT = "chunk.dispatch.entry.pursue.limit";
    private static final String CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT = "chunk.dispatch.entry.align.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MS = "chunk.dispatch.entry.pursue.timeout.ms";
    private static final String CHUNK_DISPATCH_ENTRY_BYTES_LIMIT = "chunk.dispatch.entry.bytes.LIMIT";

    private final Properties prop;

    private CoreConfig(Properties prop) {
        this.prop = prop;
    }

    public static CoreConfig fromProps(Properties properties) {
        return new CoreConfig(properties);
    }

    public String getProxyUpstreamServers() {
        return TypeTransformUtils.object2String(prop.getOrDefault(PROXY_UPSTREAM_SERVERS, "127.0.0.1:9527"));
    }

    public int getProxyHeavyLoadSubscriberThreshold() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_HEAVY_LOAD_SUBSCRIBER_THRESHOLD, 200000));
    }

    public int getProxyClientWorkerThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_CLIENT_WORKER_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getProxyClientPoolSize() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_CLIENT_POOL_SIZE, 3));
    }

    public int getProxyLeaderSyncInitialDelayMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(PROXY_LEDGER_SYNC_INITIAL_DELAY_MS, 60000));
    }

    public int getMessageSyncThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_SYNC_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
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

    public int getMetricsSampleCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(METRICS_SAMPLE_COUNT, 100));
    }

    public boolean isNetworkLogDebugEnabled() {
        return TypeTransformUtils.object2Boolean(prop.getOrDefault(NETWORK_LOG_DEBUG_ENABLED, false));
    }

    public int getWriteBufferWaterMarker() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(WRITE_BUFFER_WATER_MARK, 30 * 1024 * 1024));
    }

    public int getConnectionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CONNECTION_TIMEOUT_MS, 30000));
    }

    public String getServerId() {
        return TypeTransformUtils.object2String(prop.getOrDefault(SERVER_ID, "default"));
    }

    public String getClusterName() {
        return TypeTransformUtils.object2String(prop.getOrDefault(CLUSTER_NAME, "default"));
    }

    public String getAdvertisedAddress() {
        return TypeTransformUtils.object2String(prop.getOrDefault(ADVERTISED_ADDRESS, "localhost"));
    }

    public int getAdvertisedPort() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ADVERTISED_PORT, 8888));
    }

    public int getCompatiblePort() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(COMPATIBLE_PORT, -1));
    }

    public int getNetworkThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(NETWORK_THREAD_COUNTS, Runtime.getRuntime().availableProcessors() * 4));
    }

    public int getIoThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(IO_THREAD_COUNTS, 1));
    }

    public String getZookeeperUrl() {
        return TypeTransformUtils.object2String(prop.getOrDefault(ZOOKEEPER_URL, "localhost:2181"));
    }

    public int getSegmentRollingSize() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_ROLLING_SIZE, 4194304));
    }

    public int getZookeeperConnectionRetrySleepMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRY_SLEEP_MS, 30000));
    }

    public int getZookeeperConnectionRetries() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRIES, 3));
    }

    public int getZookeeperConnectionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_TIMEOUT_MS, 5000));
    }

    public int getZookeeperSessionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_SESSION_TIMEOUT_MS, 30000));
    }

    public int getSegmentRetainCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_RETAIN_COUNTS, 3));
    }

    public int getSegmentRetainTime() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_RETAIN_TIME, 30000));
    }

    public int getNotifyClientTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(NOTIFY_CLIENT_TIMEOUT_MS, 100));
    }

    public int getDispatchEntryLoadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getDispatchEntryFollowLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getDispatchEntryPursueLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getDispatchEntryAlignLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getDispatchEntryPursueTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_TIMEOUT_MS, 10000));
    }

    public int getChunkDispatchEntryBytesLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_BYTES_LIMIT, 65536));
    }

    public int getChunkDispatchEntryLoadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getChunkDispatchEntryFollowLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getChunkDispatchEntryPursueLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getChunkDispatchEntryAlignLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getChunkDispatchEntryPursueTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MS, 10000));
    }

    public int getCommandHandleThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageStorageThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_STORAGE_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageDispatchThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_DISPATCH_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getShutdownMaxWaitTimeMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MS, 45000));
    }

    public int getAuxThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(AUX_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }
}


