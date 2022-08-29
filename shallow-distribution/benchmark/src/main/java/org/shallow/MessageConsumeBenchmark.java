package org.shallow;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@State(Scope.Thread)
@Threads(10)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MessageConsumeBenchmark {
}
