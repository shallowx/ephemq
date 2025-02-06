package org.meteor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.meteor.metrics.netty.NettyMetrics;

import java.util.Collections;

/**
 * A suite of tests for verifying the correct binding and functionality of NettyMetrics in a MeterRegistry.
 * <p>
 * This test class validates that the NettyMetrics correctly registers gauges for monitoring Netty's
 * direct memory usage. Specifically, it checks that the gauges accurately reflect the used and maximum
 * direct memory as reported by PlatformDependent.
 */
public class NettyMetricsTests {
    /**
     * Represents the metric name for Netty's direct memory usage.
     * <p>
     * This name is used as an identifier to register gauges for tracking
     * Netty's used and maximum direct memory in a MeterRegistry.
     */
    private static final String DIRECT_MEMORY_NAME = "direct_memory";
    /**
     * The tag identifier used for categorizing the type of Netty direct memory metrics.
     * <p>
     * This tag is utilized within NettyMetrics to distinguish between different metrics such as:
     * - used direct memory
     * - maximum direct memory
     * - Java version
     * <p>
     * By specifying this tag, the metrics can be effectively grouped and filtered in the MeterRegistry.
     */
    private static final String TYPE_TAG = "type";

    /**
     * Tests the binding of NettyMetrics to a MeterRegistry and ensures that the metrics
     * for Netty's used and maximum direct memory are properly registered and reported.
     * <p>
     * The test performs the following steps:
     * - Creates a new SimpleMeterRegistry.
     * - Instantiates NettyMetrics with an empty list of tags.
     * - Binds the NettyMetrics instance to the MeterRegistry.
     * - Asserts that the used direct memory gauge in the MeterRegistry matches the value reported by PlatformDependent.
     * - Asserts that the maximum direct memory gauge in the MeterRegistry matches the value reported by PlatformDependent.
     */
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
