package org.meteor.bench.container;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark class to measure the performance of different Map implementations
 * in Java using JMH (Java Microbenchmark Harness).
 * <p>
 * This class benchmarks the following Map implementations:
 * - HashMap
 * - TreeMap
 * - ConcurrentHashMap
 * - ConcurrentSkipListMap
 * - Int2ObjectArrayMap
 * <p>
 * The benchmarks involve iterating over the entries of each map and measuring the time taken.
 * JMH annotations are used to specify the benchmark mode, warmup iterations, measurement iterations,
 * number of threads, number of forks, state scope, and output time unit.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MapBenchmark {
    /**
     * A thread-unsafe implementation of a Map used for benchmarking purposes within
     * the MapBenchmark class. The keys are integers while the values are of type Object.
     */
    private final Map<Integer, Object> hashMap = new HashMap<>();
    /**
     * A sorted map implementation that uses a red-black tree structure to order keys of type Integer.
     *
     * <p>This field is used to store mappings of integers to objects while maintaining the natural
     * ordering of the integer keys. It is part of the MapBenchmark class and utilized in performance
     * testing different map implementations.
     */
    private final Map<Integer, Object> treeMap = new TreeMap<>();
    /**
     * A thread-safe, concurrent map for storing and retrieving integer-keyed objects.
     * This map is used to benchmark the performance of concurrent access and updates
     * under various conditions.
     */
    private final Map<Integer, Object> concurrentHashMap = new ConcurrentHashMap<>();
    /**
     * A concurrent map implementation backed by a skip list, providing
     * scalable and thread-safe operations for retrieving and updating
     * mappings from integers to objects.
     * <p>
     * Used in benchmark tests to measure performance characteristics
     * compared to other map implementations.
     */
    private final Map<Integer, Object> concurrentSkipListMap = new ConcurrentSkipListMap<>();
    /**
     * A map that uses integers as keys and objects as values.
     * This implementation uses an array-based structure for storage.
     */
    private final Map<Integer, Object> int2ObjectArrayMap = new Int2ObjectArrayMap<>();

    /**
     * Initializes multiple map implementations with 1000 key-value pairs each for benchmarking purposes.
     * <p>
     * This method populates the following maps:
     * - hashMap
     * - treeMap
     * - concurrentHashMap
     * - concurrentSkipListMap
     * - int2ObjectArrayMap
     * <p>
     * Each map is loaded with 1000 entries, where the keys are integers from 0 to 999 and the values are new instances of Object.
     * This setup is intended to provide a consistent state for subsequent benchmark tests.
     */
    @Setup
    public void setUp() {
        for (int i = 0; i < 1000; i++) {
            hashMap.put(i, new Object());
            treeMap.put(i, new Object());
            concurrentHashMap.put(i, new Object());
            concurrentSkipListMap.put(i, new Object());
            int2ObjectArrayMap.put(i, new Object());
        }
    }

    /**
     * Benchmark method to iterate over all entries in a HashMap and consume the last entry.
     *
     * @param blackhole an instance of Blackhole to consume the result and prevent JVM optimizations
     */
    @Benchmark
    public void testHashMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : hashMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmark method to test the performance of iterating through the entries of a TreeMap.
     *
     * @param blackhole The Blackhole instance used to consume objects to avoid dead code elimination by the JVM.
     */
    @Benchmark
    public void testTreeMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : treeMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmark method to test the performance of iterating over a {@code ConcurrentHashMap}.
     * This method iterates over the entries of {@code concurrentHashMap} and consumes an entry using the provided {@code Blackhole}.
     *
     * @param blackhole The {@code Blackhole} instance used to consume the iteration result, preventing JVM optimizations.
     */
    @Benchmark
    public void testConcurrentHashMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : concurrentHashMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of iterating over the entries in a ConcurrentSkipListMap.
     *
     * @param blackhole The Blackhole object used to consume values and prevent optimizations
     */
    @Benchmark
    public void testConcurrentSkipListMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : concurrentSkipListMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of the "int2ObjectArrayMap".
     * <p>
     * Iterates through the map, consuming each entry using the provided blackhole.
     *
     * @param blackhole the Blackhole used to consume the object and prevent compiler optimizations.
     */
    @Benchmark
    public void testInt2ObjectArrayMap(Blackhole blackhole) {
        Object o = null;
        for (Map.Entry<Integer, Object> entry : int2ObjectArrayMap.entrySet()) {
            o = entry;
        }
        blackhole.consume(o);
    }
}
