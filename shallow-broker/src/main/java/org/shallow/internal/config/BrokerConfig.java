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

    public int getHeartInitialDelayTimeMs() {
        return object2Int(config.getOrDefault(HEARTBEAT_INITIAL_DELAY_TIME_MS, 500));
    }

    public String getProcessRoles() {
        return object2String(config.getOrDefault(PROCESS_ROLES, "controller,broker"));
    }

    public String getControllerQuorumVoters() {
        return object2String(config.getOrDefault(CONTROLLER_QUORUM_VOTERS, "shallow@127.0.0.1:9100, shallow@127.0.0.1:9200"));
    }

    public String getWorkDirectory() {
        return object2String(config.getOrDefault(WORK_DIRECTORY, "/tmp/shallow"));
    }

    public int getHeartbeatIntervalOriginTimeMs() {
        return object2Int(config.getOrDefault(HEARTBEAT_INTERVAL_ORIGIN_TIME_MS, 500));
    }

    public int getHeartbeatRandomBoundTimeMs() {
        return (getHeartbeatIntervalOriginTimeMs() + 200);
    }

    public int getHeartbeatFixedIntervalTimeMs() {
        return object2Int(getHeartbeatIntervalOriginTimeMs() + 50);
    }

    public boolean isStandAlone() {
        return object2Boolean(config.getOrDefault(STAND_ALONE, true));
    }

    public int getInvokeTimeMs() {
        return object2Int(config.getOrDefault(INVOKE_TIMEOUT_MS, 2000));
    }
}
