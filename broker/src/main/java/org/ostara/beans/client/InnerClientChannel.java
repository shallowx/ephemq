package org.ostara.beans.client;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientConfig;
import org.ostara.beans.CoreConfig;
import org.ostara.management.Manager;

import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

import static org.ostara.metrics.MetricsConstants.*;

public class InnerClientChannel extends ClientChannel {
    private final CoreConfig config;

    public InnerClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address, CoreConfig config, Manager manager) {
        super(clientConfig, channel, address);
        this.config = config;
    }

    @Override
    public void bindTo(MeterRegistry meterRegistry) {
        Gauge.builder(CHANNEL_SEMAPHORE, semaphore, Semaphore::availablePermits)
                .tag("local", channel.localAddress() == null ? StringUtil.EMPTY_STRING : channel.localAddress().toString())
                .tag("remote", channel.remoteAddress() == null ? StringUtil.EMPTY_STRING : channel.remoteAddress().toString())
                .tag(CLUSTER_TAG, config.getClusterName())
                .tag(BROKER_TAG, config.getServerId())
                .tag(ID, id)
                .register(meterRegistry);
    }
}
