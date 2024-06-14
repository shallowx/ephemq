package org.meteor.internal;

import static org.meteor.client.util.MessageConstants.CHANNEL_SEMAPHORE;
import static org.meteor.metrics.config.MetricsConstants.BROKER_TAG;
import static org.meteor.metrics.config.MetricsConstants.CLUSTER_TAG;
import static org.meteor.metrics.config.MetricsConstants.ID;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;
import java.net.SocketAddress;
import java.util.concurrent.Semaphore;
import javax.annotation.Nonnull;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.config.CommonConfig;

public class InternalClientChannel extends ClientChannel {
    private final CommonConfig configuration;

    public InternalClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address,
                                 CommonConfig configuration) {
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
