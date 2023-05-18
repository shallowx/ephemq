package org.ostara.config;

public class ConfigConstants {

    public static final String CLUSTER_NAME = "server.cluster";
    public static final String SERVER_ID = "server.id";

    public static final String EXPOSED_HOST = "exposed.host";
    public static final String EXPOSED_PORT = "exposed.port";

    public static final String PARTITION_LEADER_ASSIGN_RULE = "partition.leader.assign.rule";

    public static final String IO_THREAD_LIMIT = "io.thread.limit";
    public static final String WORK_THREAD_LIMIT = "network.thread.limit";
    public static final String OS_IS_EPOLL_PREFER = "os.epoll.prefer";

    public static final String SOCKET_WRITE_HIGH_WATER_MARK = "socket.write.high.water.mark";
    public static final String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";

    public static final String LOG_SEGMENT_LIMIT = "ledger.segment.limit";
    public static final String LOG_SEGMENT_SIZE = "ledger.segment.size";

    public static final String PROCESS_COMMAND_HANDLE_THREAD_LIMIT = "process.command.handle.thread.limit";
    public static final String MESSAGE_COMMAND_HANDLE_THREAD_LIMIT = "message.storage.handle.thread.limit";

    public static final String MESSAGE_HANDLE_THREAD_LIMIT = "message.handle.thread.limit";
    public static final String MESSAGE_HANDLE_LIMIT = "message.handle.limit";
    public static final String MESSAGE_HANDLE_ASSIGN_LIMIT = "message.handle.assign.limit";
    public static final String MESSAGE_HANDLE_ALIGN_LIMIT = "message.handle.align.limit";

    public static final String METADATA_CACHING_REFRESH_MS = "metadata.caching.refresh.ms";

    public static final String MESSAGE_CHUNK_FOLLOW_LIMIT = "message.chunk.follow.limit";
    public static final String MESSAGE_CHUNK_PURSUE_LIMIT = "message.chunk.pursue.limit";
    public static final String MESSAGE_CHUNK_ALIGN_LIMIT = "message.chunk.align.limit";
    public static final String MESSAGE_CHUNK_PURSUE_TIMEOUT_MS = "message.chunk.pursue.timeout.ms";
    public static final String MESSAGE_CHUNK_LOAD_LIMIT = "message.chunk.load.limit";
    public static final String MESSAGE_CHUNK_BYTES_LIMIT = "message.chunk.bytes.limit";

    public static final String METRICS_SAMPLE_COUNT = "metrics.sample.count";

}
