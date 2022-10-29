package org.shallow.benchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

/**
 * If no log is printed, the log level can be set to debug mode, but it may affect the performance test results
 */
@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Threads(10)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MessageConsumeBenchmark {

    @Setup
    public void setUp() {

    }


    @Benchmark
    public void pull() {

    }

    @Benchmark
    public void push() {

    }

    @TearDown
    public void shutdownGracefully() {

    }
}
