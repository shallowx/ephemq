package org.ephemq.remoting;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.config.CommonConfig;

import java.util.concurrent.atomic.LongAdder;

import static org.ephemq.metrics.config.MetricsConstants.*;

/**
 * A ChannelDuplexHandler that keeps track of active channels and logs channel activation and deactivation events.
 * The handler is sharable, meaning it can be added to multiple pipelines.
 * <p>
 * This handler also integrates with micrometer to expose the number of active channels as a gauge with specific tags.
 */
@ChannelHandler.Sharable
public class StatisticsDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StatisticsDuplexHandler.class);
    /**
     * Keeps track of the count of active channels.
     * This variable is incremented when a channel becomes active and decremented when a channel becomes inactive.
     * It is used to monitor the number of currently active channels in the system.
     */
    private final LongAdder activeChannelCount = new LongAdder();

    /**
     * Constructs a new StatisticsDuplexHandler, initializing a gauge for active channel count.
     *
     * @param config the CommonConfig instance containing the server's configuration data
     */
    public StatisticsDuplexHandler(CommonConfig config) {
        Gauge.builder(ACTIVE_CHANNEL_GAUGE_NAME, activeChannelCount, LongAdder::doubleValue)
                .tags(
                        Tags.of(
                                Tag.of(BROKER_TAG, config.getServerId()),
                                Tag.of(CLUSTER_TAG, config.getClusterName()))
                ).register(Metrics.globalRegistry);
    }

    /**
     * Handles the event when a channel becomes inactive. This method decreases the active channel count and logs the
     * channel's local and remote addresses if debugging is enabled.
     *
     * @param ctx The {@link ChannelHandlerContext} which provides various operations that enable a handler to
     *            interact with its Channel and other handlers.
     * @throws Exception If an error occurs during the event processing.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        activeChannelCount.decrement();
        if (logger.isDebugEnabled()) {
            logger.debug("Statistics duplex inactive channel, and local address[{}], remote address[{}]",
                    ctx.channel().localAddress().toString(), ctx.channel().remoteAddress().toString());
        }
        super.channelInactive(ctx);
    }

    /**
     * Handles the activation of a channel by incrementing the active channel count
     * and logging the activation event. This method is called when the channel
     * becomes active.
     * @param ctx the {@link ChannelHandlerContext} which this {@link ChannelInboundHandler}
     *            belongs to
     * @throws Exception if an error occurs while processing the event
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        activeChannelCount.increment();
        if (logger.isDebugEnabled()) {
            logger.debug("Statistics duplex active channel, and local address[{}], remote address[{}]",
                    ctx.channel().localAddress().toString(), ctx.channel().remoteAddress().toString());
        }
        super.channelActive(ctx);
    }
}
