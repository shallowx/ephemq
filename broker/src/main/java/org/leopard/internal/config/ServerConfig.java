package org.leopard.internal.config;

import org.leopard.internal.metadata.ElectAssignRule;

import java.util.Properties;

import static org.leopard.common.util.TypeUtils.*;
import static org.leopard.internal.config.ConfigConstants.*;

public class ServerConfig {

    private final Properties props;

    public static ServerConfig exchange(Properties props) {
        return new ServerConfig(props);
    }

    private ServerConfig(Properties props) {
        this.props = props;
    }

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }

    public String getServerId() {
        return object2String(props.getOrDefault(SERVER_ID, "leopard"));
    }

    public int getIoThreadLimit() {
        return object2Int(props.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    public int getNetworkThreadLimit() {
        return object2Int(props.getOrDefault(WORK_THREAD_LIMIT, availableProcessor()));
    }

    public boolean isOsEpollPrefer() {
        return object2Boolean(props.getOrDefault(OS_IS_EPOLL_PREFER, true));
    }

    public int getSocketWriteHighWaterMark() {
        return object2Int(props.getOrDefault(SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public String getExposedHost() {
        return object2String(props.getOrDefault(EXPOSED_HOST, "127.0.0.1"));
    }

    public int getExposedPort() {
        return object2Int(props.getOrDefault(EXPOSED_PORT, 9127));
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(props.getOrDefault(NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public int getInternalChannelPoolLimit() {
        return object2Int(props.getOrDefault(INTERNAL_CHANNEL_POOL_LIMIT, 1));
    }

    public String getClusterName() {
        return object2String(props.getOrDefault(CLUSTER_NAME, "shallow"));
    }

    public int getInvokeTimeMs() {
        return object2Int(props.getOrDefault(INVOKE_TIMEOUT_MS, 2000));
    }

    public int getLogSegmentLimit() {
        return object2Int(props.getOrDefault(LOG_SEGMENT_LIMIT, 2));
    }

    public int getLogSegmentSize() {
        return object2Int(props.getOrDefault(LOG_SEGMENT_SIZE, 4194304));
    }

    public int getProcessCommandHandleThreadLimit() {
        return object2Int(props.getOrDefault(PROCESS_COMMAND_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageStorageHandleThreadLimit() {
        return object2Int(props.getOrDefault(MESSAGE_COMMAND_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageHandleThreadLimit() {
        return object2Int(props.getOrDefault(MESSAGE_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageHandleLimit() {
        return object2Int(props.getOrDefault(MESSAGE_HANDLE_LIMIT, 1000));
    }

    public int getMessageHandleAssignLimit() {
        return object2Int(props.getOrDefault(MESSAGE_HANDLE_ASSIGN_LIMIT, 1000));
    }

    public int getMessageHandleAlignLimit() {
        return object2Int(props.getOrDefault(MESSAGE_HANDLE_ALIGN_LIMIT, 1000));
    }

    public String getNameserverUrl() {
        return object2String(props.getOrDefault(NAMESERVER_URL, "127.0.0.1:9100"));
    }

    public int getHeartbeatScheduleFixedDelayMs() {
        return object2Int(props.getOrDefault(HEARTBEAT_SCHEDULE_FIXED_DELAY_MS, 30000));
    }

    public String getElectAssignRule() {
        return object2String(props.getOrDefault(PARTITION_LEADER_ELECT_RULE, ElectAssignRule.AVERAGE));
    }
}
