package org.ephemq.dispatch;

import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.function.Function;

/**
 * ChunkHandler is a final class that extends AbstractHandler to manage the execution
 * and synchronization of chunk-related events.
 * It operates within a specific EventExecutor and maintains a list of ChunkSynchronization instances.
 */
final class ChunkHandler extends AbstractHandler<ChunkSynchronization, ChunkHandler> {
    /**
     * Singleton instance of ChunkHandler responsible for managing the execution and synchronization
     * of chunk-related events. This instance is used to ensure only one ChunkHandler is active within
     * the application, providing consistency and centralized control over chunk-related operations.
     */
    static final ChunkHandler INSTANCE = new ChunkHandler();
    /**
     * A list that stores instances of {@code ChunkSynchronization}.
     * This list is used to manage and keep track of synchronization events
     * related to chunks handled by the {@code ChunkHandler}.
     * The list is initialized as an empty {@code ArrayList} and is final, meaning
     * it cannot be reassigned to another list.
     */
    private final List<ChunkSynchronization> synchronizations = new ArrayList<>();

    /**
     * Constructs a new ChunkHandler instance with no associated EventExecutor.
     * By default, this will utilize a null executor, meaning no event dispatching
     * can be handled until an executor is assigned.
     */
    public ChunkHandler() {
        super(null);
    }

    /**
     * Constructs a ChunkHandler with the specified EventExecutor.
     *
     * @param executor the executor to be used for dispatching events.
     */
    public ChunkHandler(EventExecutor executor) {
        super(executor);
    }

    /**
     * This method calculates the count of event handlers for each provided {@code EventExecutor}.
     *
     * @param executors An array of {@code EventExecutor} instances to process events.
     * @param handlers  A {@code WeakHashMap} that maps event handlers to their corresponding count.
     * @return An array of integers, each representing the count of event handlers for the corresponding {@code EventExecutor}.
     */
    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<ChunkHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    /**
     * Produces a function that creates a new instance of ChunkHandler.
     *
     * @return A function that takes an EventExecutor and returns a new instance of ChunkHandler.
     */
    @Override
    Function<EventExecutor, ChunkHandler> apply() {
        return ChunkHandler::new;
    }

    /**
     * Retrieves the list of current chunk synchronizations.
     *
     * @return a list containing instances of {@link ChunkSynchronization}.
     */
    public List<ChunkSynchronization> getSynchronizations() {
        return synchronizations;
    }

    /**
     * Returns a string representation of the ChunkHandler, including details about
     * its synchronizations, subscription channels, triggered status, dispatch executor,
     * follow offset, and follow cursor.
     *
     * @return a string describing the state of the ChunkHandler instance.
     */
    @Override
    public String toString() {
        return "ChunkHandler (synchronizations=%s, subscriptionChannels=%s, triggered=%s, dispatchExecutor=%s, followOffset=%s, followCursor=%s)".formatted(synchronizations, subscriptionChannels, triggered, dispatchExecutor, followOffset, followCursor);
    }
}

