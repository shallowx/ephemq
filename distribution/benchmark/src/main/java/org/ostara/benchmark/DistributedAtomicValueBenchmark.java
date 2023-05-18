package org.ostara.benchmark;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.ostara.internal.atomic.DistributedAtomicInteger;

/**
 * If no log is printed, the log level can be set to debug mode, but it may affect the performance test results
 */
@Warmup(iterations = 3, time = 1, batchSize = 1)
@Measurement(iterations = 1, time = 100, timeUnit = TimeUnit.MILLISECONDS, batchSize = 10)
@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Threads(3)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UseLargePages",
        "-XX:+UseZGC",
        "-XX:MinHeapSize=4G",
        "-XX:InitialHeapSize=4G",
        "-XX:MaxHeapSize=4G",
        "-XX:MaxDirectMemorySize=10G",
        "-Dfile.encoding=UTF-8",
        "-Duser.timezone=Asia/Shanghai"
})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DistributedAtomicValueBenchmark {

    private DistributedAtomicInteger atomicValue;

    @Setup
    public void setUp() {
        atomicValue = new DistributedAtomicInteger();
    }

    @Benchmark
    public void testAtomicPreValue() {
        atomicValue.increment().preValue();
    }
}
