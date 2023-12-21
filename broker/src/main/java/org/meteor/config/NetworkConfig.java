package org.meteor.config;

import org.meteor.common.internal.TypeTransformUtil;

import java.util.Properties;

public class NetworkConfig {
    private static final String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    private static final String NETWORK_LOG_DEBUG_ENABLED = "network.log.debug.enabled";
    private static final String WRITE_BUFFER_WATER_MARK = "socket.write.buffer.high.watermark";
    private static final String NETWORK_THREAD_LIMIT = "network.thread.limit";
    private static final String IO_THREAD_LIMIT = "io.thread.limit";
    private static final String NOTIFY_CLIENT_TIMEOUT_MS = "notify.client.timeout.ms";
    private final Properties prop;

    public NetworkConfig(Properties prop) {
        this.prop = prop;
    }

    public boolean isNetworkLogDebugEnabled() {
        return TypeTransformUtil.object2Boolean(prop.getOrDefault(NETWORK_LOG_DEBUG_ENABLED, false));
    }

    public int getWriteBufferWaterMarker() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(WRITE_BUFFER_WATER_MARK, 30 * 1024 * 1024));
    }

    public int getConnectionTimeoutMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(CONNECTION_TIMEOUT_MS, 30000));
    }

    public int getNetworkThreadLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(NETWORK_THREAD_LIMIT, Runtime.getRuntime().availableProcessors() * 4));
    }

    public int getIoThreadLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(IO_THREAD_LIMIT, 1));
    }
    public int getNotifyClientTimeoutMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(NOTIFY_CLIENT_TIMEOUT_MS, 100));
    }
}
