package org.meteor.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;
import org.meteor.config.CommonConfig;
import org.meteor.coordinator.Coordinator;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.concurrent.Semaphore;

import static org.meteor.metrics.config.MetricsConstants.*;

public class InternalClientChannel extends ClientChannel {
    private final CommonConfig configuration;

    public InternalClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address, CommonConfig configuration, Coordinator coordinator) {
        super(clientConfig, channel, address);
        this.configuration = configuration;
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        Gauge.builder(CHANNEL_SEMAPHORE, semaphore, Semaphore::availablePermits)
                .tag("local", channel.localAddress() == null ? StringUtil.EMPTY_STRING : channel.localAddress().toString())
                .tag("remote", channel.remoteAddress() == null ? StringUtil.EMPTY_STRING : channel.remoteAddress().toString())
                .tag(CLUSTER_TAG, configuration.getClusterName())
                .tag(BROKER_TAG, configuration.getServerId())
                .tag(ID, id)
                .register(meterRegistry);
    }
}
