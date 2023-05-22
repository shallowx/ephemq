package org.ostara.core;

import org.checkerframework.checker.units.qual.C;
import org.ostara.common.util.TypeTransformUtils;

import java.util.Properties;

public class Config {
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
    private static final String MESSAGE_STORAGE_THREAD_COUNTS = "message.storage.thread.counts";

    private static final String MESSAGE_DISPATCH_THREAD_COUNTS = "message.dispatch.thread.counts";
    private static final String DISPATCH_ENTRY_LOAD_LIMIT = "dispatch.entry.load.limit";
    private static final String DISPATCH_ENTRY_FOLLOW_LIMIT = "dispatch.entry.follow.limit";
    private static final String DISPATCH_ENTRY_PURSUE_LIMIT = "dispatch.entry.pursue.limit";
    private static final String DISPATCH_ENTRY_ALIGN_LIMIT = "dispatch.entry.align.limit";
    private static final String DISPATCH_ENTRY_PURSUE_TIMEOUT_MS= "dispatch.entry.pursue.timeout.ms";

    private static final String FLUSH_BATCH = "flush.batch";
    private static final String FLUSH_BATCH_COUNT = "flush.batch.count";
    private static final String FLUSH_TIME_NANOSECONDS = "flush.time.nanoseconds";
    private static final String SHUTDOWN_MAX_WAIT_TIME_MS = "shutdown.max.wait.time.ms";
    private static final String METRICS_SAMPLE_COUNT = "metrics.sample.count";
    private static final String AUX_THREAD_COUNTS = "aux.thread.counts";

    private Properties prop;

    private Config(Properties prop) {
        this.prop = prop;
    }

    public static Config fromProps(Properties properties) {
        return new Config(properties);
    }

    public int getMetricsSampleCounts(){
        return TypeTransformUtils.object2Int(prop.getOrDefault(METRICS_SAMPLE_COUNT, 100));
    }

    public boolean isNetworkLogDebugEnabled() {
        return TypeTransformUtils.object2Boolean(prop.getOrDefault(NETWORK_LOG_DEBUG_ENABLED, false));
    }

    public int getWriteBufferWaterMarker(){
        return TypeTransformUtils.object2Int(prop.getOrDefault(WRITE_BUFFER_WATER_MARK, 30 * 1024 * 1024));
    }

    public int getConnectionTimeoutMs(){
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

    public int getCommandHandleThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageStorageThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_STORAGE_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageDispatchThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_DISPATCH_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }

    public boolean isBatch() {
        return TypeTransformUtils.object2Boolean(prop.getOrDefault(FLUSH_BATCH, false));
    }

    public int getFlushBatchCount() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(FLUSH_BATCH_COUNT, 200));
    }

    public long getFlushTimeNanoseconds() {
        return TypeTransformUtils.object2Long(prop.getOrDefault(FLUSH_TIME_NANOSECONDS, 0L));
    }

    public int getShutdownMaxWaitTimeMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MS, 45000));
    }

    public int getAuxThreadCounts() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(AUX_THREAD_COUNTS, Runtime.getRuntime().availableProcessors()));
    }
}


