package org.meteor.proxy.net;

import org.meteor.configuration.ServerConfiguration;
import org.meteor.management.Manager;
import org.meteor.net.DefaultSocketServer;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ServerConfiguration serverConfiguration, Manager manager) {
        super(serverConfiguration, manager);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, manager);
    }
}
