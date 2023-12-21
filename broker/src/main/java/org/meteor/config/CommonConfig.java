package org.meteor.config;

import org.meteor.common.internal.TypeTransformUtil;

import java.util.Properties;

public class CommonConfig {
    private static final String SERVER_ID = "server.id";
    private static final String CLUSTER_NAME = "server.cluster.name";
    private static final String ADVERTISED_ADDRESS = "server.advertised.address";
    private static final String ADVERTISED_PORT = "server.advertised.port";
    private static final String COMPATIBLE_PORT = "server.compatible.port";
    private static final String SHUTDOWN_MAX_WAIT_TIME_MS = "shutdown.max.wait.time.ms";
    private static final String AUX_THREAD_LIMIT = "aux.thread.limit";
    private static final String COMMAND_HANDLE_THREAD_LIMIT = "command.handler.thread.limit";
    private static final String SOCKET_PREFER_EPOLL = "socket.prefer.epoll";
    private final Properties prop;

    public CommonConfig(Properties prop) {
        this.prop = prop;
    }

    public String getServerId() {
        return TypeTransformUtil.object2String(prop.getOrDefault(SERVER_ID, "default"));
    }

    public boolean isSocketPreferEpoll() {
        return TypeTransformUtil.object2Boolean(prop.getOrDefault(SOCKET_PREFER_EPOLL, true));
    }

    public String getClusterName() {
        return TypeTransformUtil.object2String(prop.getOrDefault(CLUSTER_NAME, "default"));
    }

    public String getAdvertisedAddress() {
        return TypeTransformUtil.object2String(prop.getOrDefault(ADVERTISED_ADDRESS, "localhost"));
    }

    public int getAdvertisedPort() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(ADVERTISED_PORT, 8888));
    }

    public int getCompatiblePort() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(COMPATIBLE_PORT, -1));
    }

    public int getShutdownMaxWaitTimeMs() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MS, 45000));
    }

    public int getAuxThreadLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(AUX_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }
    public int getCommandHandleThreadLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }
}
