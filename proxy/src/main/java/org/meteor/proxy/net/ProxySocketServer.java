package org.meteor.proxy.net;

import org.meteor.configuration.ServerConfiguration;
import org.meteor.coordinatior.Coordinator;
import org.meteor.net.DefaultSocketServer;
import org.meteor.proxy.internal.ProxyServerConfiguration;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ProxyServerConfiguration serverConfiguration, Coordinator coordinator) {
        super(serverConfiguration, coordinator);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, coordinator);
    }
}
