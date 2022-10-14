package org.shallow.internal.config;

public interface ConfigConstants {

    String CLUSTER_NAME = "cluster";
    String SERVER_ID = "server.id";
    String EXPOSED_HOST = "exposed.host";
    String EXPOSED_PORT = "exposed.port";
    String INVOKE_TIMEOUT_MS = "invoke.timeout.ms";
    String IO_THREAD_LIMIT = "io.thread.limit";
    String WORK_THREAD_LIMIT = "network.thread.limit";
    String OS_IS_EPOLL_PREFER= "os.epoll.prefer";
    String SOCKET_WRITE_HIGH_WATER_MARK = "socket.write.high.water.mark";
    String NETWORK_LOGGING_DEBUG_ENABLED = "network.logging.debug.enabled";
    String INTERNAL_CHANNEL_POOL_LIMIT = "internal.channel.pool.limit";
    String LOG_SEGMENT_LIMIT = "ledger.segment.limit";
    String LOG_SEGMENT_SIZE = "ledger.segment.size";
    String PULL_HANDLER_RETRY_TASK_DELAY_TIME_MS = "pull.handler.retry.task.delay.time.ms";
    String PULL_HANDLE_THREAD_LIMIT = "pull.handle.thread.limit";
    String PROCESS_COMMAND_HANDLE_THREAD_LIMIT = "process.command.handle.thread.limit";
    String MESSAGE_COMMAND_HANDLE_THREAD_LIMIT = "message.storage.handle.thread.limit";
    String MESSAGE_PULL_TRANSFER_THREAD_LIMIT = "message.pull.transfer.thread.limit";
    String MESSAGE_PULL_CHAIN_THREAD_LIMIT = "message.pull.chain.thread.limit";
    String MESSAGE_PULL_BYTES_LIMIT = "message.pull.bytes.limit";
    String MESSAGE_PUSH_HANDLE_THREAD_LIMIT = "message.push.handle.thread.limit";
    String MESSAGE_PUSH_HANDLE_LIMIT = "message.push.handle.limit";
    String MESSAGE_PUSH_HANDLE_ASSIGN_LIMIT = "message.push.handle.assign.limit";
    String MESSAGE_PUSH_HANDLE_ALIGN_LIMIT = "message.push.handle.align.limit";

    String NAME_SPACE_URI = "namespace.uri";
    String CONNECTION_TIMEOUT_MS = "connection.timeout.ms";
    String SESSION_TIMEOUT_MS = "connection.session.timeout.ms";
    String CONNECTION_RETRY_SLEEP_MS = "connection.retry.sleep.ms";
    String CONNECTION_MAX_RETRIES = "connection.max.retries";
}
