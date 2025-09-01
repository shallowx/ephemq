package org.ephemq.metrics.jvm;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;

/**
 * DefaultJVMInfoMetrics is a class that collects and registers JVM-related metrics to a given
 * MeterRegistry. The metrics include important JVM properties such as version, vendor, and runtime.
 */
public class DefaultJVMInfoMetrics extends JvmInfoMetrics {
    /**
     * A collection of tags to be used for labeling JVM-related metrics.
     */
    private final Iterable<Tag> tags;

    /**
     * Constructs a DefaultJVMInfoMetrics instance with the specified tags.
     *
     * @param tags The tags to associate with the JVM info metrics.
     */
    public DefaultJVMInfoMetrics(Iterable<Tag> tags) {
        this.tags = tags;
    }

    /**
     * Binds JVM-related metrics to the given {@link MeterRegistry}.
     * The metrics include JVM version, vendor, and runtime details.
     *
     * @param registry the {@link MeterRegistry} to which the metrics will be registered
     */
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
