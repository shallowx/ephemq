package org.shallow.internal.config;

interface ConfigConstants {

    String CLUSTER_NAME = "shallow.cluster";
    String SERVER_ID = "shallow.server.id";
    String EXPOSED_HOST = "shallow.exposed.host";
    String EXPOSED_PORT = "shallow.exposed.port";

    String PROCESS_ROLES = "shallow.process.roles";
    String CONTROLLER_QUORUM_VOTERS = "shallow.controller.quorum.voters";

    String IO_THREAD_LIMIT = "shallow.io.thread.limit";
    String WORK_THREAD_LIMIT = "shallow.network.thread.limit";
    String OS_IS_EPOLL_PREFER= "shallow.os.epoll.prefer";
    String SOCKET_WRITE_HIGH_WATER_MARK = "shallow.socket.write.high.water.mark";

    String NETWORK_LOGGING_DEBUG_ENABLED = "shallow.network.logging.debug.enabled";

    String INTERNAL_CHANNEL_POOL_LIMIT = "shallow.internal.channel.pool.limit";

    String HEART_RANDOM_ORIGIN_TIME_MS = "shallow.heart.random.origin.time.ms";
    String HEART_INTERVAL_TIME_MS = "shallow.heart.interval.time.ms";

    String WORK_DIRECTORY = "shallow.work.directory";
}
