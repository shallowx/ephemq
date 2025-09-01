package org.ephemq.bench.container;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class benchmarks the performance of different list implementations.
 * It measures the time taken to iterate through the elements in various lists.
 * The lists tested are:
 * - ArrayList
 * - LinkedList
 * - ObjectArrayList
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ListBenchmark {
    /**
     * An ArrayList used for benchmarking list performance.
     * This list is pre-populated with test data in the setUp method.
     */
    private final List<Object> arrayList = new ArrayList<>();
    /**
     * A LinkedList used to benchmark performance in iteration tests.
     * It holds a series of objects and is populated during the setup phase of the benchmark.
     */
    private final List<Object> linkedList = new LinkedList<>();
    /**
     * A list implementation used in benchmarking performance tests.
     * This specifically benchmarks the `ObjectArrayList` implementation.
     * <p>
     * The `ObjectArrayList` is tested for iteration performance alongside other list types.
     */
    private final ObjectList<Object> objectArrayList = new ObjectArrayList<>();

    /**
     * Sets up the test environment by populating the lists with 1000 integer elements.
     * The following lists are populated:
     * - arrayList
     * - linkedList
     * - objectArrayList
     * This method is called before the benchmark test methods to ensure the lists are initialized.
     */
    @Setup
    public void setUp() {
        for (int i = 0; i < 1000; i++) {
            arrayList.add(i);
            linkedList.add(i);
            objectArrayList.add(i);
        }
    }

    /**
     * Benchmarks the performance of iterating through the elements of an ArrayList.
     * The method iterates over the elements of an `arrayList` and consumes the last element using a `Blackhole`.
     *
     * @param blackhole The Blackhole used to consume the object, to prevent dead code elimination by the compiler.
     */
    @Benchmark
    public void testArrayList(Blackhole blackhole) {
        Object o = null;
        for (Object value : arrayList) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of iterating through a LinkedList.
     *
     * @param blackhole a JMH Blackhole used to consume the result and avoid dead code elimination
     */
    @Benchmark
    public void testLinkedList(Blackhole blackhole) {
        Object o = null;
        for (Object value : linkedList) {
            o = value;
        }
        blackhole.consume(o);
    }

    /**
     * Benchmarks the performance of iterating through the elements of an ObjectArrayList.
     *
     * @param blackhole a Blackhole instance used to consume values and prevent
     *                  optimizations that could interfere with benchmark results
     */
    @Benchmark
    public void testObjectArrayList(Blackhole blackhole) {
        Object o = null;
        for (Object value : objectArrayList) {
            o = value;
        }
        blackhole.consume(o);
    }
}
