package org.meteor.core;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;
import org.meteor.management.Manager;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;

import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

import static org.meteor.metrics.MetricsConstants.*;

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
