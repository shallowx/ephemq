package org.ephemq.listener;

import org.ephemq.common.message.Node;

/**
 * ServerListener provides an interface for handling server lifecycle events.
 * Implementations of this interface can be registered to listen for server startup
 * and shutdown events, allowing for custom behavior to be executed during these
 * critical moments of server operation.
 * <p>
 * Methods:
 * - void onStartup(Node node): Called when the server starts up. The Node parameter
 * provides details about the server instance.
 * - void onShutdown(Node node): Called when the server shuts down. The Node parameter
 * provides details about the server instance.
 */
public interface ServerListener {
    /**
     * Invoked when the server starts up. This method allows custom actions to be performed
     * during the server startup phase.
     *
     * @param node the Node instance representing the server that is starting up, containing
     * details such as ID, host, port, registration timestamp, cluster, and state information.
     */
    void onStartup(Node node);

    /**
     * Invoked when the server shuts down, allowing for custom behavior to be executed during
     * the shutdown process. This can be used to perform tasks such as resource cleanup or
     * logging shutdown events.
     *
     * @param node the Node instance representing the server that is shutting down, providing
     *             details about the server instance such as its id, host, port, cluster, and
     *             other metadata.
     */
    void onShutdown(Node node);
}
