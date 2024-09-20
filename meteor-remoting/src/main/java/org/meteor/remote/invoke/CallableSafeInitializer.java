package org.meteor.remote.invoke;

import java.util.function.Consumer;

/**
 * Interface for managing callable initializers in a thread-safe manner.
 *
 * @param <V> the type of result produced by the initializers
 */
public interface CallableSafeInitializer<V> {
    /**
     * Returns the number of elements currently managed by this initializer.
     *
     * @return the size of the initializer
     */
    int size();

    /**
     * Checks if the collection of callable initializers is empty.
     *
     * @return true if the collection is empty; false otherwise
     */
    boolean isEmpty();

    /**
     * Retrieves a long value based on the given expiration time and rejoin feedback.
     *
     * @param expires the expiration time in milliseconds
     * @param rejoin  the feedback mechanism to be invoked
     * @return a long value representing the result based on the input parameters
     */
    long get(long expires, InvokedFeedback<V> rejoin);

    /**
     * Releases resources associated with the provided identifier and allows for custom feedback through the given consumer.
     *
     * @param rejoin   the identifier for the resources to be released
     * @param consumer a consumer that will process feedback upon releasing resources
     * @return true if the resources were successfully released, false otherwise
     */
    boolean release(long rejoin, Consumer<InvokedFeedback<V>> consumer);

    /**
     * Releases expired initializers and provides feedback through a consumer.
     *
     * @param consumer the consumer to process the feedback of each released initializer
     * @return the number of expired initializers that were released
     */
    int releaseExpired(Consumer<InvokedFeedback<V>> consumer);

    /**
     * Releases all callable initializers by invoking the provided consumer on each
     * {@link InvokedFeedback} instance.
     *
     * @param consumer a {@link Consumer} that processes each {@link InvokedFeedback} instance
     * @return the count of callable initializers that were released
     */
    int releaseAll(Consumer<InvokedFeedback<V>> consumer);
}
