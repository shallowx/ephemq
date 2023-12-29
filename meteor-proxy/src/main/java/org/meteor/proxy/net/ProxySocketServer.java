package org.meteor.proxy.net;

import org.meteor.coordinatior.Coordinator;
import org.meteor.net.DefaultSocketServer;
import org.meteor.proxy.internal.ProxyServerConfig;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ProxyServerConfig serverConfiguration, Coordinator coordinator) {
        super(serverConfiguration, coordinator);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, coordinator);
    }
}
