package org.meteor.dispatch;

import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;

/**
 * DefaultHandler is a final class that extends AbstractHandler and provides implementations
 * specific to DefaultSynchronization events. This handler manages the execution and subscription of events
 * using a provided EventExecutor. It maintains a collection of subscription markers
 * and provides the necessary mechanism to handle those subscriptions and their associated counts.
 */
final class DefaultHandler extends AbstractHandler<DefaultSynchronization, DefaultHandler> {
    /**
     * Singleton instance of the DefaultHandler.
     * <p>
     * This instance is used to manage DefaultSynchronization events, providing the
     * primary mechanism for event execution and subscription handling using a
     * provided EventExecutor. It maintains and manages the subscription markers
     * required for synchronization.
     */
    static final DefaultHandler INSTANCE = new DefaultHandler();
    /**
     * A map storing subscription markers associated with specific events.
     * The keys are integers representing unique identifiers, and the values
     * are sets of DefaultSynchronization objects associated with those keys.
     */
    private final Int2ObjectMap<Set<DefaultSynchronization>> subscriptionMarkers = new Int2ObjectOpenHashMap<>();

    /**
     * Constructs a DefaultHandler with a null EventExecutor.
     * The DefaultHandler is used to manage the execution and subscription
     * of events specific to DefaultSynchronization.
     */
    public DefaultHandler() {
        super(null);
    }

    /**
     * Constructs a DefaultHandler with the specified EventExecutor.
     *
     * @param executor the executor to be used for dispatching events.
     */
    public DefaultHandler(EventExecutor executor) {
        super(executor);
    }

    /**
     * Counts the number of times each Executor is referenced by the event handlers.
     *
     * @param executors An array of EventExecutors whose counts are to be determined.
     * @param handlers A WeakHashMap where keys are DefaultHandler instances and values are integers
     *        representing indexes corresponding to the executors array.
     * @return An array of integers where each entry represents the count of handlers referencing
     *         the corresponding executor.
     */
    @Override
    int[] getCounts(EventExecutor[] executors, WeakHashMap<DefaultHandler, Integer> handlers) {
        int[] counts = new int[executors.length];
        handlers.values().forEach(i -> counts[i]++);
        return counts;
    }

    /**
     * Generates a Function that produces new instances of DefaultHandler using the provided EventExecutor.
     *
     * @return a Function that takes an EventExecutor and returns a new DefaultHandler instance
     */
    @Override
    Function<EventExecutor, DefaultHandler> apply() {
        return DefaultHandler::new;
    }

    /**
     * Retrieves the map of subscription markers.
     *
     * @return an Int2ObjectMap containing sets of DefaultSynchronization instances, where the key is an integer representing
     * a unique marker, and the value is a set of DefaultSynchronization instances associated with that marker.
     */
    public Int2ObjectMap<Set<DefaultSynchronization>> getSubscriptionMarkers() {
        return subscriptionMarkers;
    }

    /**
     * Returns a string representation of the DefaultHandler object.
     *
     * @return a string that consists of a list of the DefaultHandler instance's state variables and their values.
     */
    @Override
    public String toString() {
        return STR."(subscriptionMarkers=\{subscriptionMarkers}, subscriptionChannels=\{subscriptionChannels}, triggered=\{triggered}, dispatchExecutor=\{dispatchExecutor}, followOffset=\{followOffset}, followCursor=\{followCursor})";
    }
}
