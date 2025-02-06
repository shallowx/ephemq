package org.meteor.metrics.netty;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.util.internal.PlatformDependent;

import javax.annotation.Nonnull;

import static org.meteor.metrics.config.MetricsConstants.DIRECT_MEMORY_NAME;
import static org.meteor.metrics.config.MetricsConstants.TYPE_TAG;

/**
 * A class that provides metrics for Netty's direct memory usage.
 * <p>
 * This class implements the {@code MeterBinder} interface to bind various gauges
 * representing Netty's direct memory metrics to a {@code MeterRegistry}.
 * <p>
 * The gauges include:
 * <ul>
 *   <li>Used direct memory</li>
 *   <li>Maximum direct memory</li>
 *   <li>Java version</li>
 * </ul>
 * <p>
 * These gauges are tagged with additional information provided at the time of instantiation.
 */
public class NettyMetrics implements MeterBinder {
    /**
     * The tags to be applied to the gauges representing Netty's direct memory metrics.
     * <p>
     * These tags provide additional contextual information, ensuring that
     * the metrics can be effectively grouped and filtered in the {@code MeterRegistry}.
     */
    private final Iterable<Tag> tags;

    /**
     * Constructs a {@code NettyMetrics} instance with the specified tags.
     *
     * @param tags an iterable collection of tags to be associated with the Netty metrics
     */
    public NettyMetrics(Iterable<Tag> tags) {
        this.tags = tags;
    }

    /**
     * Binds the netty direct memory metrics to the provided MeterRegistry.
     * <p>
     * This method registers three gauges related to Netty's direct memory usage:
     * - Used direct memory
     * - Maximum direct memory
     * - Java version information
     * <p>
     * These metrics will be tagged with user-provided tags and a predefined type tag.
     *
     * @param meterRegistry the meter registry to which the metrics should be bound
     */
    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        Gauge.builder(DIRECT_MEMORY_NAME, PlatformDependent::usedDirectMemory)
                .baseUnit("bytes")
                .tags(tags)
                .tag(TYPE_TAG, "used")
                .register(meterRegistry);

        Gauge.builder(DIRECT_MEMORY_NAME, PlatformDependent::maxDirectMemory)
                .baseUnit("bytes")
                .tags(tags)
                .tag(TYPE_TAG, "max")
                .register(meterRegistry);

        Gauge.builder(DIRECT_MEMORY_NAME, PlatformDependent::javaVersion)
                .baseUnit("bytes")
                .tags(tags)
                .tag(TYPE_TAG, "java-version")
                .register(meterRegistry);
    }
}
