package org.meteor.proxy.net;

import org.meteor.internal.MeteorServer;
import org.meteor.coordinatio.Coordinator;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Coordinator manager) {
        super(proxySocketServer, manager);
    }
}
