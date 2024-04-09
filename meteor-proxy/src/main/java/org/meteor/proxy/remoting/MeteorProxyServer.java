package org.meteor.proxy.remoting;

import org.meteor.internal.MeteorServer;
import org.meteor.support.Coordinator;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Coordinator coordinator) {
        super(proxySocketServer, coordinator);
    }
}
