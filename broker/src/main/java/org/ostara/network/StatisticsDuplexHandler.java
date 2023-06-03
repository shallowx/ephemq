package org.ostara.network;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;

import java.util.concurrent.atomic.AtomicLong;

import static org.ostara.metrics.MetricsConstants.*;

@ChannelHandler.Sharable
public class StatisticsDuplexHandler extends ChannelDuplexHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StatisticsDuplexHandler.class);

    private final AtomicLong channelCounts = new AtomicLong(0);

    public StatisticsDuplexHandler(CoreConfig config) {
        Gauge.builder(ACTIVE_CHANNEL_GAUGE_NAME, channelCounts, AtomicLong::doubleValue)
                .tags(
                    Tags.of(
                    Tag.of(BROKER_TAG, config.getServerId()),
                    Tag.of(CLUSTER_TAG, config.getClusterName()))
                ).register(Metrics.globalRegistry);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        channelCounts.decrementAndGet();
        super.channelInactive(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelCounts.incrementAndGet();
        super.channelActive(ctx);
    }
}
