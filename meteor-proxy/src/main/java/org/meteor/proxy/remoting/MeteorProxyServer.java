package org.meteor.proxy.remoting;

import org.meteor.internal.MeteorServer;
import org.meteor.support.Manager;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Manager coordinator) {
        super(proxySocketServer, coordinator);
    }
}
