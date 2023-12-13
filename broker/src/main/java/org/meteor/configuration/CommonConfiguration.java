package org.meteor.configuration;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class CommonConfiguration {
    private static final String SERVER_ID = "server.id";
    private static final String CLUSTER_NAME = "server.cluster.name";
    private static final String ADVERTISED_ADDRESS = "server.advertised.address";
    private static final String ADVERTISED_PORT = "server.advertised.port";
    private static final String COMPATIBLE_PORT = "server.compatible.port";
    private static final String SHUTDOWN_MAX_WAIT_TIME_MS = "shutdown.max.wait.time.ms";
    private static final String AUX_THREAD_LIMIT = "aux.thread.limit";
    private static final String COMMAND_HANDLE_THREAD_LIMIT = "command.handler.thread.limit";
    private final Properties prop;

    public CommonConfiguration(Properties prop) {
        this.prop = prop;
    }

    public String getServerId() {
        return TypeTransformUtils.object2String(prop.getOrDefault(SERVER_ID, "default"));
    }

    public String getClusterName() {
        return TypeTransformUtils.object2String(prop.getOrDefault(CLUSTER_NAME, "default"));
    }

    public String getAdvertisedAddress() {
        return TypeTransformUtils.object2String(prop.getOrDefault(ADVERTISED_ADDRESS, "localhost"));
    }

    public int getAdvertisedPort() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ADVERTISED_PORT, 8888));
    }

    public int getCompatiblePort() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(COMPATIBLE_PORT, -1));
    }

    public int getShutdownMaxWaitTimeMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MS, 45000));
    }

    public int getAuxThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(AUX_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }
    public int getCommandHandleThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }
}
