package org.ostara.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;

public class DefaultJVMInfoMetrics extends JvmInfoMetrics {
    private final Iterable<Tag> tags;

    public DefaultJVMInfoMetrics(Iterable<Tag> tags) {
        this.tags = tags;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Tags combinedTags = Tags.of(tags)
                .and("version", System.getProperty("java.runtime.version", "unknown"))
                .and("vendor", System.getProperty("java.vm.vendor", "unknown"))
                .and("runtime", System.getProperty("java.runtime.name", "unknown"));

        Gauge.builder("jvm.info", () -> 1L)
                .description("JVM version info")
                .tags(combinedTags)
                .strongReference(true)
                .register(registry);
    }
}
