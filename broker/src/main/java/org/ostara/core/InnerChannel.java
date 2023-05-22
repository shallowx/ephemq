package org.ostara.core;

import io.netty.channel.Channel;
import org.ostara.client.ClientConfig;
import org.ostara.client.internal.ClientChannel;
import org.ostara.management.Manager;

import java.net.SocketAddress;

public class InnerChannel extends ClientChannel {

    private Config config;
    private Manager manager;

    public InnerChannel(ClientConfig clientConfig, Channel channel, SocketAddress address, Config config, Manager manager) {
        super(clientConfig, channel, address);
        this.config = config;
        this.manager = manager;
    }
}
