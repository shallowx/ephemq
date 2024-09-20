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

/**
 * This class is used to benchmark the performance of various Set implementations.
 * It includes setups and benchmark tests for HashSet, ConcurrentSkipListSet,
 * CopyOnWriteArraySet, ObjectArraySet, and ObjectLinkedOpenHashSet.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SetBenchmark {
    /**
     * A HashSet used for benchmarking its performance in various set operations.
     * This set is populated with 1000 integer elements during the setup phase.
     */
    private final Set<Object> hashset = new HashSet<>();
    /**
     * A thread-safe, sorted set implementation based on ConcurrentSkipListSet.
     * It is used within the SetBenchmark class to benchmark the performance
     * of this set compared to other Set implementations.
     */
    private final Set<Object> concurrentSkipListSet = new ConcurrentSkipListSet<>();
    /**
     * A thread-safe set implementation from the java.util.concurrent package,
     * which is designed for use in scenarios where the set is frequently iterated
     * and only occasionally modified. This implementation is optimized for
     * concurrency by copying the entire array on each write operation, making it best suited for read-heavy use cases.
     */
    private final Set<Object> copyOnWriteArraySet = new CopyOnWriteArraySet<>();
    /**
     * A set used in benchmarking tests to evaluate the performance of the
     * ObjectArraySet implementation. This set is populated with integers
     * during the setup phase of the benchmark.
     */
    private final Set<Object> objectSet = new ObjectArraySet<>();
    /**
     * A Set implementation used for benchmarking the performance of
     * ObjectLinkedOpenHashSet within the SetBenchmark class.
     */
    private final Set<Object> objectObjectLinkedOpenHashSet = new ObjectLinkedOpenHashSet<>();

    /**
     * Initializes several Set implementations before running benchmark tests.
     * <p>
     * Adds 1000 integer elements (from 0 to 999) to the following sets:
     * - HashSet
     * - ConcurrentSkipListSet
     * - CopyOnWriteArraySet
     * - ObjectArraySet
     * - ObjectLinkedOpenHashSet
     * <p>
     * This setup method ensures each set is populated with the same data
     * before performance measurements are performed.
     */
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

    /**
     * Benchmarks the performance of iterating over a HashSet and consuming the last element using a Blackhole.
     *
     * @param blackhole a utility to prevent JIT optimizations by consuming variables that would otherwise be unused
     */
    @Benchmark
    public void testHashSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : hashset) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of iterating over a ConcurrentSkipListSet
     * and consuming the last element using a `Blackhole`.
     *
     * @param blackhole a `Blackhole` instance used to consume the result
     */
    @Benchmark
    public void testSkipSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : concurrentSkipListSet) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of iteration over a CopyOnWriteArraySet.
     *
     * @param blackhole The Blackhole instance used to consume the result and avoid dead code elimination.
     */
    @Benchmark
    public void testCopyOnWriteSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : copyOnWriteArraySet) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmark method to measure the performance of iterating over objectObjectLinkedOpenHashSet.
     *
     * @param blackhole The Blackhole instance used to consume values to prevent dead code elimination.
     */
    @Benchmark
    public void testObjectArraySet(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectObjectLinkedOpenHashSet) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the iteration performance over an ObjectLinkedOpenHashSet.
     *
     * @param blackhole A placeholder object used to consume the result and prevent optimizations.
     */
    @Benchmark
    public void testObjectLinkedOpenHashSet(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectSet) {
            o = value;
        }
        blackhole.consume(o);
    }
}
