package org.meteor.config;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Boolean;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2String;
import io.netty.util.NettyRuntime;
import java.util.Properties;

public class CommonConfig {
    private static final String SERVER_ID = "server.id";
    private static final String CLUSTER_NAME = "server.cluster.name";
    private static final String ADVERTISED_ADDRESS = "server.advertised.address";
    private static final String ADVERTISED_PORT = "server.advertised.port";
    private static final String COMPATIBLE_PORT = "server.compatible.port";
    private static final String SHUTDOWN_MAX_WAIT_TIME_MILLISECONDS = "shutdown.max.wait.time.milliseconds";
    private static final String AUX_THREAD_LIMIT = "aux.thread.limit";
    private static final String COMMAND_HANDLE_THREAD_LIMIT = "command.handler.thread.limit";
    private static final String SOCKET_PREFER_EPOLL = "socket.prefer.epoll";
    // https://netty.io/wiki/thread-affinity.html
    private static final String THREAD_AFFINITY_ENABLED = "thread.affinity.enabled";
    private final Properties prop;

    public CommonConfig(Properties prop) {
        this.prop = prop;
    }

    public boolean isThreadAffinityEnabled() {
        return object2Boolean(prop.getOrDefault(THREAD_AFFINITY_ENABLED, "false"));
    }

    public String getServerId() {
        return object2String(prop.getOrDefault(SERVER_ID, "default"));
    }

    public boolean isSocketPreferEpoll() {
        return object2Boolean(prop.getOrDefault(SOCKET_PREFER_EPOLL, true));
    }

    public String getClusterName() {
        return object2String(prop.getOrDefault(CLUSTER_NAME, "default"));
    }

    public String getAdvertisedAddress() {
        return object2String(prop.getOrDefault(ADVERTISED_ADDRESS, "localhost"));
    }

    public int getAdvertisedPort() {
        return object2Int(prop.getOrDefault(ADVERTISED_PORT, 8888));
    }

    public int getCompatiblePort() {
        return object2Int(prop.getOrDefault(COMPATIBLE_PORT, -1));
    }

    public int getShutdownMaxWaitTimeMilliseconds() {
        return object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MILLISECONDS, 45000));
    }

    public int getAuxThreadLimit() {
        return object2Int(prop.getOrDefault(AUX_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    public int getCommandHandleThreadLimit() {
        return object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }
}
