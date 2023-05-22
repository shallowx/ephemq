package org.ostara.core.inner;

import io.netty.channel.Channel;
import org.ostara.client.ClientConfig;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientListener;
import org.ostara.core.Config;
import org.ostara.management.Manager;

import java.net.SocketAddress;

public class InnerClient extends Client {
    private Config config;
    private Manager manager;

    public InnerClient(String name, ClientConfig clientConfig, ClientListener listener, Config config, Manager manager) {
        super(name, clientConfig, listener);
        this.config = config;
        this.manager = manager;
    }

    @Override
    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InnerClientChannel(clientConfig, channel, address, config, manager);
    }
}
