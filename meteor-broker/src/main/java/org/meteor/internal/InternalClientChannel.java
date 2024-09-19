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

/**
 * InternalClientChannel extends the ClientChannel class and provides additional configuration
 * through the CommonConfig object. It is designed for internal use within the system and
 * includes additional metadata for monitoring and metrics.
 */
public class InternalClientChannel extends ClientChannel {
    /**
     * Provides the configuration settings for the InternalClientChannel.
     * The configuration is encapsulated within an instance of the CommonConfig class,
     * which manages various server configuration parameters.
     */
    private final CommonConfig configuration;

    /**
     * Constructs an instance of InternalClientChannel.
     *
     * @param clientConfig  The configuration for the client, specifying various parameters such as timeouts and buffer sizes.
     * @param channel       The Netty channel used for communication.
     * @param address       The socket address the channel is bound to.
     * @param configuration Additional common configuration that provides more metadata and settings for the channel.
     */
    public InternalClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address,
                                 CommonConfig configuration) {
        super(clientConfig, channel, address);
        this.configuration = configuration;
    }

    /**
     * Binds the current InternalClientChannel instance to the provided MeterRegistry
     * for metrics monitoring. This method creates and registers a Gauge to track
     * the available permits of the semaphore associated with this channel.
     *
     * @param meterRegistry the MeterRegistry instance to bind the metrics to
     */
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
