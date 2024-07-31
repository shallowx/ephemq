package org.meteor.remoting;

import static org.meteor.metrics.config.MetricsConstants.ACTIVE_CHANNEL_GAUGE_NAME;
import static org.meteor.metrics.config.MetricsConstants.BROKER_TAG;
import static org.meteor.metrics.config.MetricsConstants.CLUSTER_TAG;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.LongAdder;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.CommonConfig;

@ChannelHandler.Sharable
public class StatisticsDuplexHandler extends ChannelDuplexHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StatisticsDuplexHandler.class);
    private final LongAdder activeChannelCount = new LongAdder();

    public StatisticsDuplexHandler(CommonConfig config) {
        Gauge.builder(ACTIVE_CHANNEL_GAUGE_NAME, activeChannelCount, LongAdder::doubleValue)
                .tags(
                        Tags.of(
                                Tag.of(BROKER_TAG, config.getServerId()),
                                Tag.of(CLUSTER_TAG, config.getClusterName()))
                ).register(Metrics.globalRegistry);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        activeChannelCount.decrement();
        super.channelInactive(ctx);
        if (logger.isDebugEnabled()) {
            logger.debug("Statistics duplex inactive channel, and local address[{}], remote address[{}]",
                    ctx.channel().localAddress().toString(), ctx.channel().remoteAddress().toString());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        activeChannelCount.increment();
        super.channelActive(ctx);
        if (logger.isDebugEnabled()) {
            logger.debug("Statistics duplex active channel, and local address[{}], remote address[{}]",
                    ctx.channel().localAddress().toString(), ctx.channel().remoteAddress().toString());
        }
    }
}
