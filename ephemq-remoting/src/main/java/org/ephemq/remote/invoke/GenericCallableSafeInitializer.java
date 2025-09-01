package org.ephemq.remote.invoke;

import io.netty.util.Recycler;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;

import java.util.function.Consumer;

/**
 * A thread-safe initializer that manages callable initializers with expiration capabilities.
 * This class ensures that callable initializers can be safely invoked and managed in a
 * concurrent environment.
 *
 * @param <V> the type of result produced by the initializers
 */
public final class GenericCallableSafeInitializer<V> implements CallableSafeInitializer<V> {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(GenericCallableSafeInitializer.class);
    /**
     * A map that holds references to {@link Holder} instances keyed by long identifiers.
     * This map is used within the {@link GenericCallableSafeInitializer} class to manage
     * state and lifecycle of various holders based on their unique long keys.
     */
    private final Long2ObjectMap<Holder> holders;
    /**
     * A unique identifier for a request within the {@code GenericCallableSafeInitializer}.
     */
    private long requestId;

    /**
     * Constructs a GenericCallableSafeInitializer with a default capacity.
     * The default capacity is set to 2048.
     */
    public GenericCallableSafeInitializer() {
        this(2048);
    }

    /**
     * Constructs a new GenericCallableSafeInitializer with the specified initial capacity.
     *
     * @param capacity the initial capacity of the internal holder map
     */
    public GenericCallableSafeInitializer(int capacity) {
        this.holders = new Long2ObjectLinkedOpenHashMap<>(capacity);
    }

    /**
     * Returns the current number of holders managed by the GenericCallableSafeInitializer.
     *
     * @return the number of holders
     */
    @Override
    public int size() {
        return holders.size();
    }

    /**
     * Checks if the collection of callable initializers is empty.
     *
     * @return true if the collection is empty; false otherwise
     */
    @Override
    public boolean isEmpty() {
        return holders.isEmpty();
    }

    /**
     * Retrieves a unique request identifier for the given expiry time and associated feedback.
     *
     * @param expires  the time in milliseconds after which the request expires
     * @param feedback the feedback mechanism to be invoked upon request completion
     * @return a unique request identifier as a long value
     */
    @Override
    public long get(long expires, InvokedFeedback<V> feedback) {
        if (null == feedback) {
            return 0L;
        }

        long nextRequestId = nextRequestId();
        holders.put(nextRequestId, Holder.newHolder(expires, feedback));
        return nextRequestId;
    }

    /**
     * Releases resources associated with the specified feedback identifier and processes feedback with the given consumer.
     *
     * @param feedback the identifier for the resources to be released
     * @param consumer a consumer that processes feedback for the released resources
     * @return true if the resources were successfully released, false otherwise
     */
    @Override
    public boolean release(long feedback, Consumer<InvokedFeedback<V>> consumer) {
        if (feedback == 0L) {
            return false;
        }

        Holder holder = holders.remove(feedback);
        if (null == holder) {
            return false;
        }

        if (holder.isValid() && null != consumer) {
            doConsume(holder, consumer);
        }
        holder.recycle();
        return true;
    }

    /**
     * Releases all callable initializers by invoking the provided consumer on each
     * {@link InvokedFeedback} instance.
     *
     * @param consumer a {@link Consumer} that processes each {@link InvokedFeedback} instance
     * @return the count of callable initializers that were released
     */
    @Override
    public int releaseAll(Consumer<InvokedFeedback<V>> consumer) {
        if (isEmpty()) {
            return 0;
        }

        var count = 0;
        ObjectIterator<Long2ObjectMap.Entry<Holder>> iterator = holders.long2ObjectEntrySet().iterator();
        while (iterator.hasNext()) {
            Holder holder = iterator.next().getValue();
            if (holder.isValid() && null != consumer) {
                doConsume(holder, consumer);
            }

            iterator.remove();
            holder.recycle();
            count++;
        }
        return count;
    }


    /**
     * Releases expired holders and provides feedback through a consumer if specified.
     *
     * @param consumer the consumer to process the feedback of each released holder
     * @return the number of expired holders that were released
     */
    @Override
    public int releaseExpired(Consumer<InvokedFeedback<V>> consumer) {
        if (isEmpty()) {
            return 0;
        }

        long now = System.currentTimeMillis();

        var count = 0;
        ObjectIterator<Long2ObjectMap.Entry<Holder>> iterator = holders.long2ObjectEntrySet().iterator();
        while (iterator.hasNext()) {
            Holder holder = iterator.next().getValue();
            boolean valid = holder.isValid();

            if (valid && holder.expires > now) {
                continue;
            }

            if (valid && null != consumer) {
                doConsume(holder, consumer);
            }

            iterator.remove();
            holder.recycle();
            count++;
        }
        return count;
    }

    /**
     * Processes the given {@link Holder} using the specified {@link Consumer}.
     * If an exception occurs during processing, logs the error and marks the feedback as failed.
     *
     * @param holder the {@link Holder} instance containing the feedback to be processed
     * @param consumer the {@link Consumer} that will process the {@link InvokedFeedback} instance
     */
    private void doConsume(Holder holder, Consumer<InvokedFeedback<V>> consumer) {
        @SuppressWarnings("unchecked")
        InvokedFeedback<V> feedback = (GenericInvokedFeedback<V>) holder.feedback;
        try {
            consumer.accept(feedback);
        } catch (Throwable cause) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consume message failure", cause);
            }
            feedback.failure(cause);
        }
    }

    /**
     * Generates the next unique request identifier.
     *
     * @return the next request identifier. If the incremented value of requestId is zero, it increments again to avoid returning zero.
     */
    private long nextRequestId() {
        return ++requestId == 0 ? ++requestId : requestId;
    }

    private static final class Holder {
        /**
         * A static final instance of the Recycler class for managing instances of the Holder class.
         * The Recycler helps in reusing Holder objects to reduce the overhead of object creation and garbage collection.
         * <p>
         * The Recycler ensures that a new Holder object is created only if there are no recycled instances available.
         */
        private static final Recycler<Holder> RECYCLER = new Recycler<>() {
            @Override
            protected Holder newObject(Handle<Holder> handle) {
                return new Holder(handle);
            }
        };

        /**
         * Handle associated with the current {@code Holder} instance for managing
         * its lifecycle and recycling.
         */
        private final Recycler.Handle<Holder> handle;
        /**
         * The timestamp indicating when this holder instance expires.
         * When the expires time is reached or surpassed, the holder is considered invalid.
         */
        private long expires;
        /**
         * A reference to an InvokedFeedback<?> object representing the asynchronous feedback mechanism.
         * This variable holds the state and methods to monitor and control the completion status of a
         * particular operation.
         */
        private InvokedFeedback<?> feedback;

        /**
         * Constructs a new Holder instance with the provided Recycler handle.
         *
         * @param handle the Recycler handle associated with this Holder
         */
        public Holder(Recycler.Handle<Holder> handle) {
            this.handle = handle;
        }

        /**
         * Creates a new holder instance with the specified expiration time and feedback mechanism.
         *
         * @param expires the time in milliseconds after which the holder expires
         * @param feedback the feedback mechanism to be associated with this holder
         * @return a new instance of Holder with the specified properties
         */
        private static Holder newHolder(long expires, InvokedFeedback<?> feedback) {
            Holder instance = RECYCLER.get();
            instance.expires = expires;
            instance.feedback = feedback;
            return instance;
        }

        /**
         * Recycles the internal resources of the Holder instance.
         * <p>
         * This method is responsible for resetting the feedback property
         * to null and returning the current Holder instance back to the
         * recycler for reuse.
         */
        private void recycle() {
            this.feedback = null;
            handle.recycle(this);
        }

        /**
         * Checks if the feedback is not null and not completed.
         *
         * @return true if the feedback is not null and not completed, false otherwise
         */
        private boolean isValid() {
            return null != feedback && !feedback.isCompleted();
        }
    }
}
