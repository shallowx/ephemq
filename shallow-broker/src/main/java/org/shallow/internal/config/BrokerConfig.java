package org.shallow.internal.config;

import java.util.Properties;

import static org.shallow.internal.config.ConfigConstants.*;
import static org.shallow.util.TypeUtil.*;

public class BrokerConfig {

    private final Properties config;

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

    public int getIoThreadLimit(){
        return object2Int(config.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    public int getNetworkThreadLimit(){
        return object2Int(config.getOrDefault(WORK_THREAD_LIMIT, availableProcessor()));
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

    public int getInternalChannelPoolLimit() {
        return object2Int(config.getOrDefault(INTERNAL_CHANNEL_POOL_LIMIT, 1));
    }

    public String getClusterName() {
        return object2String(config.getOrDefault(CLUSTER_NAME, "shallow"));
    }

    public int getHeartIntervalTimeMs() {
        return object2Int(config.getOrDefault(HEART_INTERVAL_TIME_MS, 30000));
    }

    public String getProcessRoles() {
        return object2String(config.getOrDefault(PROCESS_ROLES, "controller,broker"));
    }

    public String getControllerQuorumVoters() {
        return object2String(config.getOrDefault(CONTROLLER_QUORUM_VOTERS, "127.0.0.1:9100"));
    }

    public String getWorkDirectory() {
        return object2String(config.getOrDefault(WORK_DIRECTORY, "/tmp/shallow"));
    }

    public int getHeartbeatRandomOriginTimeMs() {
        return object2Int(config.getOrDefault(HEART_RANDOM_ORIGIN_TIME_MS, 150));
    }

    public int getHeartbeatRandomBoundTimeMs() {
        return (getHeartbeatRandomOriginTimeMs() + 150);
    }
}
