package org.meteor.proxy.remoting;

import org.meteor.coordinatior.Coordinator;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.proxy.internal.ProxyServerConfig;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ProxyServerConfig serverConfiguration, Coordinator coordinator) {
        super(serverConfiguration, coordinator);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, coordinator);
    }
}
