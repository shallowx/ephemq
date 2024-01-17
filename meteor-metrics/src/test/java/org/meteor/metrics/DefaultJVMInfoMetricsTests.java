package org.meteor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.metrics.jvm.DefaultJVMInfoMetrics;

public class DefaultJVMInfoMetricsTests {
    @Test
    public void testJVMInfoMetrics() {
        MeterRegistry registry = new SimpleMeterRegistry();
        DefaultJVMInfoMetrics jvmInfoMetrics = new DefaultJVMInfoMetrics(Tags.of("environment", "test"));
        jvmInfoMetrics.bindTo(registry);
        Assertions.assertEquals(1L, registry.get("jvm.info").gauge().value());
        Assertions.assertEquals("JVM version info", registry.get("jvm.info").gauge().getId().getDescription());
        Assertions.assertEquals("test", registry.get("jvm.info").gauge().getId().getTag("environment"));
    }
}
