package org.shallow;

import org.openjdk.jmh.annotations.*;
import org.shallow.metadata.atomic.DistributedAtomicInteger;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Measurement(iterations = 1, time = 3)
@Threads(3)
@Fork(1)
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
