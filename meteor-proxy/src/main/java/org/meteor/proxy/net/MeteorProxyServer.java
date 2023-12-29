package org.meteor.proxy.net;

import org.meteor.internal.MeteorServer;
import org.meteor.coordinatior.Coordinator;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Coordinator coordinator) {
        super(proxySocketServer, coordinator);
    }
}
