package org.ostara.core.inner;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;
import org.ostara.client.ClientConfig;
import org.ostara.client.internal.ClientChannel;
import org.ostara.core.Config;
import org.ostara.management.Manager;
import org.ostara.metrics.MetricsConstants;

import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

public class InnerClientChannel extends ClientChannel {
    private final Config config;

    public InnerClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address, Config config, Manager manager) {
        super(clientConfig, channel, address);
        this.config = config;
    }

    @Override
    public void bindTo(MeterRegistry meterRegistry) {
        Gauge.builder(CHANNEL_SEMAPHORE, semaphore, Semaphore::availablePermits)
                .tag("local", channel.localAddress() == null ? StringUtil.EMPTY_STRING : channel.localAddress().toString())
                .tag("remote", channel.remoteAddress() == null ? StringUtil.EMPTY_STRING : channel.remoteAddress().toString())
                .tag(MetricsConstants.CLUSTER_TAG, config.getClusterName())
                .tag(MetricsConstants.BROKER_TAG, config.getServerId())
                .tag("id", id)
                .register(meterRegistry);
    }
}
