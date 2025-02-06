package org.meteor.proxy.remoting;

import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.Manager;

/**
 * ProxySocketServer extends DefaultSocketServer to provide proxy-specific functionality.
 * This class uses a customized channel initializer to handle proxy server configurations.
 */
public class ProxySocketServer extends DefaultSocketServer {
    /**
     * Constructs a ProxySocketServer instance with the specified server configuration and manager.
     * This server is intended to initialize and manage the channel for a proxy server.
     *
     * @param serverConfiguration The configuration settings for the proxy server, encapsulated in a ProxyServerConfig object.
     * @param manager The Manager instance responsible for managing server-related operations and tasks.
     */
    public ProxySocketServer(ProxyServerConfig serverConfiguration, Manager manager) {
        super(serverConfiguration, manager);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, manager);
    }
}
