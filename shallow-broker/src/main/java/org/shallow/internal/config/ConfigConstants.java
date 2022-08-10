package org.shallow.internal.config;

public interface ConfigConstants {

    String CLUSTER_NAME = "shallow.cluster";
    String SERVER_ID = "shallow.server.id";
    String EXPOSED_HOST = "shallow.exposed.host";
    String EXPOSED_PORT = "shallow.exposed.port";

    String STAND_ALONE = "shallow.stand.alone";
    String PROCESS_ROLES = "shallow.process.roles";
    String CONTROLLER_QUORUM_VOTERS = "shallow.controller.quorum.voters";

    String IO_THREAD_LIMIT = "shallow.io.thread.limit";
    String WORK_THREAD_LIMIT = "shallow.network.thread.limit";
    String OS_IS_EPOLL_PREFER= "shallow.os.epoll.prefer";
    String SOCKET_WRITE_HIGH_WATER_MARK = "shallow.socket.write.high.water.mark";

    String NETWORK_LOGGING_DEBUG_ENABLED = "shallow.network.logging.debug.enabled";

    String INTERNAL_CHANNEL_POOL_LIMIT = "shallow.internal.channel.pool.limit";

    String HEARTBEAT_INITIAL_DELAY_TIME_MS = "shallow.heartbeat.initial.delay.time.ms";
    String HEARTBEAT_INTERVAL_ORIGIN_TIME_MS = "shallow.heartbeat.interval.origin.time.ms";
    String INVOKE_TIMEOUT_MS = "shallow.invoke.timeout.ms";
    String WORK_DIRECTORY = "shallow.work.directory";
}
