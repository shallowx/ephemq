package org.meteor.proxy.remoting;

import org.meteor.coordinator.Coordinator;
import org.meteor.internal.MeteorServer;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Coordinator coordinator) {
        super(proxySocketServer, coordinator);
    }
}
