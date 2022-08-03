package org.shallow.internal;

import java.util.Properties;

import static org.shallow.util.TypeUtil.*;

public class MetadataConfig {
    private final Properties config;

    private static final String SERVER_ID = "shallow.nameserver.id";
    private static final String CLUSTER_NAME = "shallow.nameserver.cluster";
    private static final String IO_THREAD_WHOLES = "shallow.nameserver.io.thread.wholes";
    private static final String WORK_THREAD_WHOLES = "shallow.nameserver.network.thread.wholes";
    private static final String OS_IS_EPOLL_PREFER= "shallow.nameserver.os.epoll.prefer";
    private static final String SOCKET_WRITE_HIGH_WATER_MARK = "shallow.nameserver.socket.write.high.water.mark";
    private static final String EXPOSED_HOST = "shallow.nameserver.exposed.host";
    private static final String EXPOSED_PORT = "shallow.nameserver.exposed.port";
    private static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.nameserver.logging.debug.enabled";
    private static final String WORK_DIRECTORY = "shallow.nameserver.work.directory";
    private static final String NODE_HEART_DELAY_TIME_MS = "shallow.nameserver.check.heart.delay.time.ms";
    private static final String NODE_HEART_INTERVAL_TIME_MS = "shallow.nameserver.heart.max.interval.time.ms";
    private static final String NODE_LAST_AVAILABLE_TIME_MS = "shallow.nameserver.check.last.available.time.ms";
    private static final String SHALLOW_NAMESERVER_CONTROLLER = "shallow.nameserver.controller";

    public static MetadataConfig exchange(Properties properties) {
        return new MetadataConfig(properties);
    }

    private MetadataConfig(Properties config) {
        this.config = config;
    }

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }

    public String getServerId() {
        return object2String(config.getOrDefault(SERVER_ID, "shallow-nameserver"));
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
        return object2Int(config.getOrDefault(EXPOSED_PORT, 9100));
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(config.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public String getWorkDirectory() {
        return object2String(config.getOrDefault(WORK_DIRECTORY, "/tmp/shallow"));
    }

    public int getCheckHeartDelayTimeMs() {
        // the value must be < getHeartMaxIntervalTimeMs()
        return object2Int(config.getOrDefault(NODE_HEART_DELAY_TIME_MS, 20000));
    }

    public int getHeartMaxIntervalTimeMs() {
        return object2Int(config.getOrDefault(NODE_HEART_INTERVAL_TIME_MS, 40000));
    }

    public int getHearCheckLastAvailableTimeMs() {
        return object2Int(config.getOrDefault(NODE_LAST_AVAILABLE_TIME_MS, 120000));
    }

    public boolean isController() {
        return object2Boolean(config.getOrDefault(SHALLOW_NAMESERVER_CONTROLLER, false));
    }
}
