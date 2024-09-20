package org.meteor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.metrics.jvm.DefaultJVMInfoMetrics;

/**
 * Test class for verifying the functionality of DefaultJVMInfoMetrics.
 * This class includes tests to ensure that JVM-related metrics are correctly registered and
 * labeled in the MeterRegistry.
 */
public class DefaultJVMInfoMetricsTests {
    /**
     * Tests the functionality of the DefaultJVMInfoMetrics class to ensure metrics related to the JVM
     * are correctly registered and labeled in the MeterRegistry.
     * <p>
     * The method verifies:
     * - The metric "jvm.info" is present in the registry with a value of 1.
     * - The metric description is "JVM version info".
     * - The metric is tagged with "environment" set to "test".
     * <p>
     * This is accomplished by:
     * - Creating a SimpleMeterRegistry instance.
     * - Creating a DefaultJVMInfoMetrics instance with specific tags.
     * - Binding the DefaultJVMInfoMetrics instance to the registry.
     * - Asserting that the registered metric meets the expected values and tags.
     */
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
