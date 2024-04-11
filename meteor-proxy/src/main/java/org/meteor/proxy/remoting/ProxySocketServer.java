package org.meteor.proxy.remoting;

import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.Manager;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ProxyServerConfig serverConfiguration, Manager coordinator) {
        super(serverConfiguration, coordinator);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, coordinator);
    }
}
