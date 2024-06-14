package org.meteor.proxy.remoting;

import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.Manager;

public class ProxySocketServer extends DefaultSocketServer {
    public ProxySocketServer(ProxyServerConfig serverConfiguration, Manager manager) {
        super(serverConfiguration, manager);
        this.serviceChannelInitializer = new ProxyServerChannelInitializer(serverConfiguration, manager);
    }
}
