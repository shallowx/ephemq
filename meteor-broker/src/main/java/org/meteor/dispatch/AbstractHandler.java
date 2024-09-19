package org.meteor.dispatch;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

/**
 * An abstract class that handles the execution of a collection of events using a specific executor.
 * This class provides a general framework for managing and processing events, leaving the
 * implementation specifics to subclasses.
 *
 * @param <E> The type of event being handled.
 * @param <T> The type of handler managing the event execution.
 */
abstract class AbstractHandler<E, T> {
    /**
     * A `ConcurrentMap` that holds the subscription channels along with their respective event types.
     * This map ensures thread-safe access and modifications, and is used to manage the active
     * subscriptions within the handler. Each key in the map represents a channel and its corresponding
     * event type.
     */
    protected final ConcurrentMap<Channel, E> subscriptionChannels = new ConcurrentHashMap<>();
    /**
     * Indicates whether the event handler has been triggered.
     * This boolean flag helps in controlling the state of event handling
     * within the AbstractHandler class to ensure that actions are taken
     * only when necessary.
     */
    protected final AtomicBoolean triggered = new AtomicBoolean(false);
    /**
     * The executor responsible for handling the dispatch of events.
     * This executor is used to manage and execute the processing of events
     * associated with the handler.
     */
    protected final EventExecutor dispatchExecutor;
    /**
     * The {@code followOffset} variable represents the current offset being followed by the handler in the log.
     * It is a volatile field to ensure visibility of its updates across multiple threads.
     * This offset helps in tracking the position within the event stream or ledger.
     */
    protected volatile Offset followOffset;
    /**
     * A volatile variable that holds a reference to the current LedgerCursor.
     * This cursor is used to navigate through ledger segments and read records
     * from the underlying storage. The cursor can be used to seek to a specific
     * offset and read the next available record or chunk of records.
     */
    protected volatile LedgerCursor followCursor;

    /**
     * Constructs an AbstractHandler with the specified EventExecutor.
     *
     * @param executor the executor to be used for dispatching events.
     */
    public AbstractHandler(EventExecutor executor) {
        this.dispatchExecutor = executor;
    }

    /**
     * This method calculates the count of event handlers for each provided {@code EventExecutor}.
     *
     * @param executors An array of {@code EventExecutor} instances to process events.
     * @param handlers  A {@code WeakHashMap} that maps event handlers to their corresponding count.
     * @return An array of integers, each representing the count of event handlers for the corresponding {@code EventExecutor}.
     */
    abstract int[] getCounts(EventExecutor[] executors, WeakHashMap<T, Integer> handlers);

    /**
     * Applies a given EventExecutor to produce a handler of type T.
     *
     * @return A function that takes an EventExecutor and returns a handler of type T.
     */
    abstract Function<EventExecutor, T> apply();

    /**
     * Creates a new handler instance for managing event execution.
     *
     * @param handlers A map of existing handlers with their associated executor index.
     * @param executors An array of executors available for handling events.
     * @return A new handler instance allocated to an appropriate executor.
     */
    protected T newHandler(WeakHashMap<T, Integer> handlers, EventExecutor[] executors) {
        return newHandler(executors, handlers, apply());
    }

    /**
     * Creates a new handler for the provided executors and registers it in the handlers map.
     *
     * @param executors An array of {@code EventExecutor} instances available for handling events.
     * @param handlers A {@code WeakHashMap} holding existing handlers and their associated executor indexes.
     * @param f A function that creates a new handler instance given an {@code EventExecutor}.
     * @return A new handler instance of type {@code T} created and associated with an executor.
     */
    private T newHandler(EventExecutor[] executors, WeakHashMap<T, Integer> handlers, Function<EventExecutor, T> f) {
        int[] countArray = getCounts(executors, handlers);
        int index = index(countArray);
        T result = f.apply(executors[index]);
        handlers.put(result, index);
        return result;
    }

    /**
     * Determines the index of the first zero value in the given countArray.
     * If no zero value is found, it returns the index of the smallest value.
     *
     * @param countArray An array of integer counts representing some metrics.
     * @return The index of the first occurrence of zero value or the index of the smallest value in the array.
     */
    private int index(int[] countArray) {
        int index = 0;
        if (countArray[index] > 0) {
            for (int i = 1, length = countArray.length; i < length; i++) {
                int v = countArray[i];
                if (v == 0) {
                    index = i;
                    break;
                }

                if (v < countArray[i]) {
                    index = i;
                }
            }
        }
        return index;
    }

    /**
     * Returns the triggered status of the handler.
     *
     * @return An AtomicBoolean indicating whether the handler has been triggered.
     */
    public AtomicBoolean getTriggered() {
        return triggered;
    }

    /**
     * Retrieves the current follow offset.
     *
     * @return the current follow offset.
     */
    public Offset getFollowOffset() {
        return followOffset;
    }

    /**
     * Sets the follow offset to the specified value.
     *
     * @param followOffset the new follow offset to be set
     */
    public void setFollowOffset(Offset followOffset) {
        this.followOffset = followOffset;
    }

    /**
     * Retrieves the current follow cursor, which tracks the position in the
     * ledger for processing events.
     *
     * @return the current follow cursor.
     */
    public LedgerCursor getFollowCursor() {
        return followCursor;
    }

    /**
     * Sets the cursor used to follow the ledger.
     *
     * @param followCursor The LedgerCursor instance that will be set to follow the ledger.
     */
    public void setFollowCursor(LedgerCursor followCursor) {
        this.followCursor = followCursor;
    }

    /**
     * Returns a concurrent map of subscription channels and their associated events.
     *
     * @return ConcurrentMap<Channel, E> the map representing the subscription channels and their associated events
     */
    public ConcurrentMap<Channel, E> getSubscriptionChannels() {
        return subscriptionChannels;
    }

    /**
     * Retrieves the executor responsible for handling event dispatches.
     *
     * @return the instance of {@link EventExecutor} used for dispatching events.
     */
    public EventExecutor getDispatchExecutor() {
        return dispatchExecutor;
    }
}
