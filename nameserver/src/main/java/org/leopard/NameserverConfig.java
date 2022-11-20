package org.leopard;

import java.util.Properties;

import static org.leopard.common.util.TypeUtils.*;

public class NameserverConfig {

    private final Properties props;

    private static final String EXPOSED_HOST = "exposed.host";
    private static final String EXPOSED_PORT = "exposed.port";
    private static final String SOCKET_WRITE_HIGH_WATER_MARK = "socket.write.high.water.mark";
    private static final String IO_THREAD_LIMIT = "io.thread.limit";
    private static final String WORK_THREAD_LIMIT = "network.thread.limit";
    private static final String OS_IS_EPOLL_PREFER = "os.epoll.prefer";
    private static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";
    private static final String HEARTBEAT_DELAY_PERIOD = "heartbeat.delay.period";

    public static NameserverConfig exchange(Properties props) {
        return new NameserverConfig(props);
    }

    private NameserverConfig(Properties props) {
        this.props = props;
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(props.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public String getExposedHost() {
        return object2String(props.getOrDefault(EXPOSED_HOST, "127.0.0.1"));
    }

    public int getExposedPort() {
        return object2Int(props.getOrDefault(EXPOSED_PORT, 9100));
    }

    public int getSocketWriteHighWaterMark() {
        return object2Int(props.getOrDefault(SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public int getIoThreadLimit() {
        return object2Int(props.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    public int getWorkThreadLimit() {
        return object2Int(props.getOrDefault(WORK_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }

    public boolean isOsEpollPrefer() {
        return object2Boolean(props.getOrDefault(OS_IS_EPOLL_PREFER, true));
    }

    public int getHeartbeatDelayPeriod() {
        return object2Int(props.getOrDefault(HEARTBEAT_DELAY_PERIOD, 30000));
    }
}
