package org.ephemq.config;

import io.netty.util.NettyRuntime;

import java.util.Properties;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.*;

/**
 * The CommonConfig class provides a unified configuration management for server settings.
 * It initializes various server configuration parameters from a Properties object and
 * provides methods to retrieve these values in appropriate types.
 */
public class CommonConfig {
    /**
     * The unique identifier for the server configured in the system.
     * This identifier is used to distinguish between different servers in a cluster.
     */
    private static final String SERVER_ID = "server.id";
    /**
     * The name of the server cluster.
     */
    private static final String CLUSTER_NAME = "server.cluster.name";
    /**
     * Property key for the server's advertised address.
     */
    private static final String ADVERTISED_ADDRESS = "server.advertised.address";
    /**
     * Constant representing the property key for the advertised port of the server.
     * This key is used to retrieve the port number on which the server advertises its presence.
     */
    private static final String ADVERTISED_PORT = "server.advertised.port";
    /**
     * The configuration property key for defining the compatible port of the server.
     */
    private static final String COMPATIBLE_PORT = "server.compatible.port";
    /**
     * Configuration key for the maximum wait time (in milliseconds) during the shutdown process.
     */
    private static final String SHUTDOWN_MAX_WAIT_TIME_MILLISECONDS = "shutdown.max.wait.time.milliseconds";
    /**
     * Configuration key representing the limit for auxiliary threads in the system.
     * Used to control the maximum number of auxiliary threads that can be created.
     */
    private static final String AUX_THREAD_LIMIT = "aux.thread.limit";
    /**
     * A configuration property key representing the maximum number of threads
     * allocated for command handling.
     */
    private static final String COMMAND_HANDLE_THREAD_LIMIT = "command.handler.thread.limit";
    /**
     * Specifies whether the system should prefer using the epoll mechanism for socket handling.
     * This configuration is often used to improve network performance on Linux-based systems
     * by leveraging the epoll I/O event notification facility.
     */
    private static final String SOCKET_PREFER_EPOLL = "socket.prefer.epoll";
    /**
     * Specifies whether the system should prefer using the io_uring mechanism for socket handling.
     * This configuration is often used to improve network performance on Linux-based systems
     * by leveraging the io_uring I/O event notification facility.
     */
    private static final String SOCKET_PREFER_IO_URING = "socket.prefer.iouring";
    /**
     * Specifies whether the system should prefer using the affinity mechanism for socket handling.
     * This configuration is often used to improve network performance on Linux-based systems
     * by leveraging the affinity I/O event notification facility.
     */
    private static final String SOCKET_PREFER_AFFINITY = "socket.prefer.affinity";

    /**
     * A configuration key that indicates whether thread affinity is enabled.
     * Thread affinity is a performance optimization where certain threads are
     * bound to specific CPUs, aiming to improve cache efficiency and reduce context switches.
     * <p>
     * For more information, visit the Netty documentation on thread affinity:
     * <a href="https://netty.io/wiki/thread-affinity.html">...</a>
     */
    private static final String THREAD_AFFINITY_ENABLED = "thread.affinity.enabled";

    /**
     * Configuration properties for the CommonConfig class.
     * <p>
     * This variable holds a set of key-value pairs that provide the necessary
     * configuration for the CommonConfig class. These properties are immutable
     * after being set through the constructor, ensuring consistent configuration
     * throughout the lifecycle of the object.
     */
    private final Properties prop;

    /**
     * Constructs a CommonConfig instance with the specified properties.
     *
     * @param prop the properties to be used for configuration
     */
    public CommonConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Checks if thread affinity is enabled in the configuration.
     *
     * @return true if thread affinity is enabled, false otherwise
     */
    public boolean isThreadAffinityEnabled() {
        return object2Boolean(prop.getOrDefault(THREAD_AFFINITY_ENABLED, "false"));
    }

    /**
     * Retrieves the server ID from the properties configuration.
     * If the server ID is not set, it returns a default value.
     *
     * @return The server ID as a string. If not present, returns "default".
     */
    public String getServerId() {
        return object2String(prop.getOrDefault(SERVER_ID, "default"));
    }

    /**
     * Checks if the socket should prefer using Epoll.
     * <p>
     * This method retrieves the value associated with the
     * SOCKET_PREFER_EPOLL property from the configuration.
     * If the property is not explicitly set, it defaults to true.
     *
     * @return true if socket should prefer using Epoll, false otherwise.
     */
    public boolean isSocketPreferEpoll() {
        return object2Boolean(prop.getOrDefault(SOCKET_PREFER_EPOLL, true));
    }

    /**
     * Checks if the socket should prefer using io_uring.
     * <p>
     * This method retrieves the value associated with the
     * SOCKET_PREFER_IO_URING property from the configuration.
     * If the property is not explicitly set, it defaults to true.
     *
     * @return true if socket should prefer using io_uring, false otherwise.
     */
    public boolean isSocketPreferIoUring() {
        return object2Boolean(prop.getOrDefault(SOCKET_PREFER_IO_URING, false));
    }

    /**
     * Checks if the socket should prefer using affinity.
     * <p>
     * This method retrieves the value associated with the
     * SOCKET_PREFER_AFFINITY property from the configuration.
     * If the property is not explicitly set, it defaults to true.
     *
     * @return true if socket should prefer using affinity, false otherwise.
     */
    public boolean isSocketPreferAffinity() {
        return object2Boolean(prop.getOrDefault(SOCKET_PREFER_AFFINITY, false));
    }

    /**
     * Retrieves the name of the cluster from the configuration properties.
     *
     * @return the cluster name if specified; otherwise, returns "default"
     */
    public String getClusterName() {
        return object2String(prop.getOrDefault(CLUSTER_NAME, "default"));
    }

    /**
     * Retrieves the advertised address from the properties or defaults to "localhost" if not set.
     *
     * @return the advertised address as a String
     */
    public String getAdvertisedAddress() {
        return object2String(prop.getOrDefault(ADVERTISED_ADDRESS, "localhost"));
    }

    /**
     * Retrieves the advertised port for the server.
     *
     * @return the advertised port number, or 8888 if the advertised port is not explicitly specified.
     */
    public int getAdvertisedPort() {
        return object2Int(prop.getOrDefault(ADVERTISED_PORT, 8888));
    }

    /**
     * Retrieves the compatible port from the properties. If the compatible port is not set,
     * the default value of 8889 is returned.
     *
     * @return the compatible port or the default value of 8889 if not set.
     */
    public int getCompatiblePort() {
        return object2Int(prop.getOrDefault(COMPATIBLE_PORT, 8889));
    }

    /**
     * Retrieves the maximum wait time for a shutdown process in milliseconds.
     *
     * @return the maximum wait time for a shutdown in milliseconds. If the property
     * SHUTDOWN_MAX_WAIT_TIME_MILLISECONDS is not set, it returns a default value of 45000 milliseconds.
     */
    public int getShutdownMaxWaitTimeMilliseconds() {
        return object2Int(prop.getOrDefault(SHUTDOWN_MAX_WAIT_TIME_MILLISECONDS, 45000));
    }

    /**
     * Retrieves the auxiliary thread limit from the configuration properties.
     *
     * @return the auxiliary thread limit, if specified, or the number
     *         of available processors in the system if the property is not set
     */
    public int getAuxThreadLimit() {
        return object2Int(prop.getOrDefault(AUX_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    /**
     * Retrieves the configured thread limit for handling commands. If not explicitly set, returns the
     * available number of processors on the current machine.
     *
     * @return the thread limit for handling commands
     */
    public int getCommandHandleThreadLimit() {
        return object2Int(prop.getOrDefault(COMMAND_HANDLE_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }
}
