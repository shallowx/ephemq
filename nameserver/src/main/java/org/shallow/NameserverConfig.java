package org.shallow;

import java.util.Properties;

import static org.shallow.common.util.TypeUtil.*;

public class NameserverConfig {

    private final Properties config;

    private static final String EXPOSED_HOST = "exposed.host";
    private static final String EXPOSED_PORT = "exposed.port";
    private static final String SOCKET_WRITE_HIGH_WATER_MARK = "socket.write.high.water.mark";
    private static final String IO_THREAD_LIMIT = "io.thread.limit";
    private static final String WORK_THREAD_LIMIT = "network.thread.limit";
    private static final String OS_IS_EPOLL_PREFER= "os.epoll.prefer";
    private static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";

    public static NameserverConfig exchange(Properties properties) {
        return new NameserverConfig(properties);
    }

    private NameserverConfig(Properties config) {
        this.config = config;
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(config.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public String getExposedHost() {
        return object2String(config.getOrDefault(EXPOSED_HOST, "127.0.0.1"));
    }

    public int getExposedPort() {
        return object2Int(config.getOrDefault(EXPOSED_PORT, 9100));
    }

    public int getSocketWriteHighWaterMark() {
        return object2Int(config.getOrDefault(SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public int getIoThreadLimit() {
        return object2Int(config.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    public int getWorkThreadLimit() {
        return object2Int(config.getOrDefault(WORK_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }

    public boolean isOsEpollPrefer() {
        return object2Boolean(config.getOrDefault(OS_IS_EPOLL_PREFER, true));
    }
}
