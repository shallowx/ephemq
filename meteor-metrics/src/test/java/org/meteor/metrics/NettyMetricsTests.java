package org.meteor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.meteor.metrics.netty.NettyMetrics;

import java.util.Collections;

public class NettyMetricsTests {
    private static final String DIRECT_MEMORY_NAME = "direct_memory";
    private static final String TYPE_TAG = "type";

    @Test
    void testNettyMetrics() {
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        NettyMetrics nettyMetrics = new NettyMetrics(Collections.emptyList());
        nettyMetrics.bindTo(meterRegistry);
        Assertions.assertEquals(PlatformDependent.usedDirectMemory(), meterRegistry.get(DIRECT_MEMORY_NAME)
                .tags(TYPE_TAG, "used").gauge().value());
        Assertions.assertEquals(PlatformDependent.maxDirectMemory(), meterRegistry.get(DIRECT_MEMORY_NAME)
                .tags(TYPE_TAG, "max").gauge().value());
    }
}
