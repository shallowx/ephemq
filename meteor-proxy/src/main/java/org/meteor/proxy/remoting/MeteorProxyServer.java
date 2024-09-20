package org.meteor.proxy.remoting;

import org.meteor.internal.MeteorServer;
import org.meteor.support.Manager;

/**
 * The MeteorProxyServer class extends the MeteorServer class to manage the lifecycle
 * of a proxy socket server. It incorporates functionalities for starting
 * and shutting down the proxy socket server.
 */
public class MeteorProxyServer extends MeteorServer {
    /**
     * Constructs a new MeteorProxyServer with the specified ProxySocketServer and Manager.
     *
     * @param proxySocketServer the ProxySocketServer to be managed by the MeteorProxyServer
     * @param manager           the Manager to handle and coordinate the operations of the server
     */
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Manager manager) {
        super(proxySocketServer, manager);
    }
}
