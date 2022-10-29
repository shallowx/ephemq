package org.shallow.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.State;
import org.shallow.internal.atomic.DistributedAtomicInteger;

import java.util.concurrent.TimeUnit;

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
    public void testAtomicIntegerValue() {
        Integer incrementPreValue = atomicValue.increment().preValue();
    }
}
