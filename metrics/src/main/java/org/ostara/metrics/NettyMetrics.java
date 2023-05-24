package org.ostara.metrics;

import groovyjarjarantlr4.v4.runtime.misc.NotNull;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.util.internal.PlatformDependent;

import static org.ostara.metrics.MetricsConstants.DIRECT_MEMORY_NAME;
import static org.ostara.metrics.MetricsConstants.TYPE_TAG;

public class NettyMetrics implements MeterBinder {

    private final Iterable<Tag> tags;

    public NettyMetrics(Iterable<Tag> tags) {
        this.tags = tags;
    }

    @Override
    public void bindTo(@NotNull MeterRegistry meterRegistry) {
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
