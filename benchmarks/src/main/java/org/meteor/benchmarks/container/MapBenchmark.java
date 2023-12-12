package org.meteor.benchmarks.container;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MapBenchmark {
    private final Map<Integer, Object> hashMap = new HashMap<>();
    private final Map<Integer, Object> treeMap = new TreeMap<>();
    private final Map<Integer, Object> concurrentHashMap = new ConcurrentHashMap<>();
    private final Map<Integer, Object> concurrentSkipListMap = new ConcurrentSkipListMap<>();
    private final Map<Integer, Object> Int2ObjectArrayMap = new Int2ObjectArrayMap<>();

    @Setup
    public void setUp() {
        for (int i = 0; i < 1000; i++) {
            hashMap.put(i, new Object());
            treeMap.put(i, new Object());
            concurrentHashMap.put(i, new Object());
            concurrentSkipListMap.put(i, new Object());
            Int2ObjectArrayMap.put(i, new Object());
        }
    }

    @Benchmark
    public void testHashMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : hashMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testTreeMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : treeMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testConcurrentHashMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : concurrentHashMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testConcurrentSkipListMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : concurrentSkipListMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    @Benchmark
    public void testInt2ObjectArrayMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : Int2ObjectArrayMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }
}
