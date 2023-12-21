package org.meteor.bench.container;

import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SetBenchmark {
    private final Set<Object> hashset = new HashSet<>();
    private final Set<Object> concurrentSkipListSet = new ConcurrentSkipListSet<>();
    private final Set<Object> copyOnWriteArraySet = new CopyOnWriteArraySet<>();
    private final Set<Object> objectSet = new ObjectArraySet<>();
    private final Set<Object> objectObjectLinkedOpenHashSet = new ObjectLinkedOpenHashSet<>();

    @Setup
    public void setUp() {
        for (int i = 0; i < 1000; i++) {
            hashset.add(i);
            concurrentSkipListSet.add(i);
            copyOnWriteArraySet.add(i);
            objectSet.add(i);
            objectObjectLinkedOpenHashSet.add(i);
        }
    }

    @Benchmark
    public void testHashSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : hashset) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testSkipSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : concurrentSkipListSet) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testCopyOnWriteSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : copyOnWriteArraySet) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testObjectArraySet(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectObjectLinkedOpenHashSet) {
            o = value;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testObjectLinkedOpenHashSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectSet) {
            o = value;
        }
        blackhole.consume(o);
    }
}
