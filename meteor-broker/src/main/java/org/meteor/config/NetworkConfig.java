package org.meteor.config;

import io.netty.util.NettyRuntime;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Boolean;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class NetworkConfig {
    private static final String CONNECTION_TIMEOUT_MILLISECONDS = "connection.timeout.milliseconds";
    private static final String NETWORK_LOG_DEBUG_ENABLED = "network.log.debug.enabled";
    private static final String WRITE_BUFFER_WATER_MARK = "socket.write.buffer.high.watermark";
    private static final String NETWORK_THREAD_LIMIT = "network.thread.limit";
    private static final String IO_THREAD_LIMIT = "io.thread.limit";
    private static final String NOTIFY_CLIENT_TIMEOUT_MILLISECONDS = "notify.client.timeout.milliseconds";
    private final Properties prop;

    public NetworkConfig(Properties prop) {
        this.prop = prop;
    }

    public boolean isNetworkLogDebugEnabled() {
        return object2Boolean(prop.getOrDefault(NETWORK_LOG_DEBUG_ENABLED, false));
    }

    public int getWriteBufferWaterMarker() {
        return object2Int(prop.getOrDefault(WRITE_BUFFER_WATER_MARK, 30 * 1024 * 1024));
    }

    public int getConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(CONNECTION_TIMEOUT_MILLISECONDS, 30000));
    }

    public int getNetworkThreadLimit() {
        return object2Int(prop.getOrDefault(NETWORK_THREAD_LIMIT, NettyRuntime.availableProcessors() * 4));
    }

    public int getIoThreadLimit() {
        return object2Int(prop.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    public int getNotifyClientTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(NOTIFY_CLIENT_TIMEOUT_MILLISECONDS, 100));
    }
}
