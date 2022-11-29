package org.leopard.internal.config;

import static org.leopard.common.util.TypeUtils.object2Boolean;
import static org.leopard.common.util.TypeUtils.object2Int;
import static org.leopard.common.util.TypeUtils.object2String;
import static org.leopard.internal.config.ConfigConstants.CLUSTER_NAME;
import static org.leopard.internal.config.ConfigConstants.EXPOSED_HOST;
import static org.leopard.internal.config.ConfigConstants.EXPOSED_PORT;
import static org.leopard.internal.config.ConfigConstants.IO_THREAD_LIMIT;
import static org.leopard.internal.config.ConfigConstants.LOG_SEGMENT_LIMIT;
import static org.leopard.internal.config.ConfigConstants.LOG_SEGMENT_SIZE;
import static org.leopard.internal.config.ConfigConstants.MESSAGE_COMMAND_HANDLE_THREAD_LIMIT;
import static org.leopard.internal.config.ConfigConstants.MESSAGE_HANDLE_ALIGN_LIMIT;
import static org.leopard.internal.config.ConfigConstants.MESSAGE_HANDLE_ASSIGN_LIMIT;
import static org.leopard.internal.config.ConfigConstants.MESSAGE_HANDLE_LIMIT;
import static org.leopard.internal.config.ConfigConstants.MESSAGE_HANDLE_THREAD_LIMIT;
import static org.leopard.internal.config.ConfigConstants.METADATA_CACHING_REFRESH_MS;
import static org.leopard.internal.config.ConfigConstants.NETWORK_LOGGING_DEBUG_ENABLED;
import static org.leopard.internal.config.ConfigConstants.OS_IS_EPOLL_PREFER;
import static org.leopard.internal.config.ConfigConstants.PARTITION_LEADER_ASSIGN_RULE;
import static org.leopard.internal.config.ConfigConstants.PROCESS_COMMAND_HANDLE_THREAD_LIMIT;
import static org.leopard.internal.config.ConfigConstants.SERVER_ID;
import static org.leopard.internal.config.ConfigConstants.SOCKET_WRITE_HIGH_WATER_MARK;
import static org.leopard.internal.config.ConfigConstants.WORK_THREAD_LIMIT;
import java.util.Properties;
import org.leopard.internal.metadata.PartitionAssignRule;

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

    public String getClusterName() {
        return object2String(props.getOrDefault(CLUSTER_NAME, "leopard"));
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

    public String getElectAssignRule() {
        return object2String(props.getOrDefault(PARTITION_LEADER_ASSIGN_RULE, PartitionAssignRule.RANDOM));
    }

    public int getMetadataRefreshMs() {
        return object2Int(props.getOrDefault(METADATA_CACHING_REFRESH_MS, 60000));
    }
}
