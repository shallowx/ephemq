package org.ostara.benchmarks.container;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ListBenchmark {
    private final List<Object> arrayList = new ArrayList<>();
    private final List<Object> linkedList = new LinkedList<>();
    private final ObjectList<Object> objectArrayList = new ObjectArrayList<>();

    @Setup
    public void setUp() {
        for (int i = 0; i < 1000; i++) {
            arrayList.add(i);
            linkedList.add(i);
            objectArrayList.add(i);
        }
    }

    @Benchmark
    public void testArrayList(Blackhole blackhole) {
        Object o = null;
        for (Object value : arrayList) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testLinkedList(Blackhole blackhole) {
        Object o = null;
        for (Object value : linkedList) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testObjectArrayList(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectArrayList) {
            o = value;
        }
        blackhole.consume(o);
    }
}
