package org.shallow.internal.config;

import java.util.Properties;

import static org.shallow.util.TypeUtil.*;
import static org.shallow.util.TypeUtil.object2String;

public class BrokerConfig {

    private final Properties config;

    private static final String SERVER_ID = "shallow.server.id";
    private static final String CLUSTER_NAME = "shallow.cluster";
    private static final String IO_THREAD_WHOLES = "shallow.io.thread.wholes";
    private static final String WORK_THREAD_WHOLES = "shallow.network.thread.wholes";
    private static final String OS_IS_EPOLL_PREFER= "shallow.os.epoll.prefer";
    private static final String SOCKET_WRITE_HIGH_WATER_MARK = "shallow.socket.write.high.water.mark";
    private static final String EXPOSED_HOST = "shallow.exposed.host";
    private static final String EXPOSED_PORT = "shallow.exposed.port";
    private static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";
    private static final String SHALLOW_NAMESERVER_URL = "shallow.nameserver.url";
    private static final String SHALLOW_INTERNAL_CHANNEL_POOL_LIMIT = "shallow.internal.channel.pool.limit";
    private static final String NODE_HEART_SEND_INTERVAL_TIME_MS = "shallow.node.send.heart.interval.time.ms";

    public static BrokerConfig exchange(Properties properties) {
        return new BrokerConfig(properties);
    }

    private BrokerConfig(Properties config) {
        this.config = config;
    }

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }

    public String getServerId() {
        return object2String(config.getOrDefault(SERVER_ID, "shallow"));
    }

    public int getIoThreadWholes(){
        return object2Int(config.getOrDefault(IO_THREAD_WHOLES, 1));
    }

    public int getNetworkThreadWholes(){
        return object2Int(config.getOrDefault(WORK_THREAD_WHOLES, availableProcessor()));
    }

    public boolean isOsEpollPrefer(){
        return object2Boolean(config.getOrDefault(OS_IS_EPOLL_PREFER, false));
    }

    public int getSocketWriteHighWaterMark(){
        return object2Int(config.getOrDefault(SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public String getExposedHost(){
        return object2String(config.getOrDefault(EXPOSED_HOST, "127.0.0.1"));
    }

    public int getExposedPort(){
        return object2Int(config.getOrDefault(EXPOSED_PORT, 7730));
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(config.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public int getInternalChannelPoolLimit() {
        return object2Int(config.getOrDefault(SHALLOW_INTERNAL_CHANNEL_POOL_LIMIT, 1));
    }

    public String getNameserverUrl() {
        return object2String(config.getOrDefault(SHALLOW_NAMESERVER_URL, "127.0.0.1:9100"));
    }

    public String getClusterName() {
        return object2String(config.getOrDefault(CLUSTER_NAME, "shallow"));
    }

    public int getHeartSendIntervalTimeMs() {
        return object2Int(config.getOrDefault(NODE_HEART_SEND_INTERVAL_TIME_MS, 30000));
    }
}
