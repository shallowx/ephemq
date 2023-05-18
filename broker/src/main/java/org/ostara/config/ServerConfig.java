package org.ostara.config;

import static org.ostara.common.util.TypeTransformUtils.object2Boolean;
import static org.ostara.common.util.TypeTransformUtils.object2Int;
import static org.ostara.common.util.TypeTransformUtils.object2String;
import java.util.Properties;
import org.ostara.context.PartitionAssignRule;

public class ServerConfig {

    private final Properties props;

    // only use to metrics config
    public Properties getProps() {
        return props;
    }

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
        return object2String(props.getOrDefault(ConfigConstants.SERVER_ID, "ostara"));
    }

    public int getIoThreadLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.IO_THREAD_LIMIT, 1));
    }

    public int getNetworkThreadLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.WORK_THREAD_LIMIT, availableProcessor()));
    }

    public boolean isOsEpollPrefer() {
        return object2Boolean(props.getOrDefault(ConfigConstants.OS_IS_EPOLL_PREFER, true));
    }

    public int getSocketWriteHighWaterMark() {
        return object2Int(props.getOrDefault(ConfigConstants.SOCKET_WRITE_HIGH_WATER_MARK, 20 * 1024 * 1024));
    }

    public String getExposedHost() {
        return object2String(props.getOrDefault(ConfigConstants.EXPOSED_HOST, "127.0.0.1"));
    }

    public int getExposedPort() {
        return object2Int(props.getOrDefault(ConfigConstants.EXPOSED_PORT, 9127));
    }

    public boolean isNetworkLoggingDebugEnabled() {
        return object2Boolean(props.getOrDefault(ConfigConstants.NETWORK_LOGGING_DEBUG_ENABLED, false));
    }

    public String getClusterName() {
        return object2String(props.getOrDefault(ConfigConstants.CLUSTER_NAME, "ostara"));
    }

    public int getLogSegmentLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.LOG_SEGMENT_LIMIT, 2));
    }

    public int getLogSegmentSize() {
        return object2Int(props.getOrDefault(ConfigConstants.LOG_SEGMENT_SIZE, 4194304));
    }

    public int getProcessCommandHandleThreadLimit() {
        return object2Int(
                props.getOrDefault(ConfigConstants.PROCESS_COMMAND_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageStorageHandleThreadLimit() {
        return object2Int(
                props.getOrDefault(ConfigConstants.MESSAGE_COMMAND_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageHandleThreadLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_HANDLE_THREAD_LIMIT, availableProcessor()));
    }

    public int getMessageHandleLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_HANDLE_LIMIT, 1000));
    }

    public int getMessageHandleAssignLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_HANDLE_ASSIGN_LIMIT, 1000));
    }

    public int getMessageHandleAlignLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_HANDLE_ALIGN_LIMIT, 1000));
    }

    public String getElectAssignRule() {
        return object2String(
                props.getOrDefault(ConfigConstants.PARTITION_LEADER_ASSIGN_RULE, PartitionAssignRule.RANDOM));
    }

    public int getMetadataRefreshMs() {
        return object2Int(props.getOrDefault(ConfigConstants.METADATA_CACHING_REFRESH_MS, 60000));
    }

    public int getChunkFollowLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_FOLLOW_LIMIT, 100));
    }

    public int getChunkPursueLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_PURSUE_LIMIT, 500));
    }

    public int getChunkAlignLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_ALIGN_LIMIT, 2000));
    }

    public int getChunkPursueTimeoutMs() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_PURSUE_TIMEOUT_MS, 100));
    }

    public int getChunkLoadLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_LOAD_LIMIT, 50));
    }

    public int getChunkBytesLimit() {
        return object2Int(props.getOrDefault(ConfigConstants.MESSAGE_CHUNK_BYTES_LIMIT, 65536));
    }

    public int getMetricsSampleCount(){
        return object2Int(props.getOrDefault(ConfigConstants.METRICS_SAMPLE_COUNT, 100));
    }
}
