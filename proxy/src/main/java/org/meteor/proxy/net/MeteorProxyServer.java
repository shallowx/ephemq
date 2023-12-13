package org.meteor.proxy.net;

import org.meteor.core.MeteorServer;
import org.meteor.management.Manager;

public class MeteorProxyServer extends MeteorServer {
    public MeteorProxyServer(ProxySocketServer proxySocketServer, Manager manager) {
        super(proxySocketServer, manager);
    }
}
