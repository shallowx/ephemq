package org.meteor.remote.invoke;

import io.netty.util.Recycler;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.function.Consumer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

public final class GenericCallableSafeInitializer<V> implements CallableSafeInitializer<V> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(GenericCallableSafeInitializer.class);

    private final Long2ObjectMap<Holder> holders;
    private long requestId;

    public GenericCallableSafeInitializer() {
        this(2048);
    }

    public GenericCallableSafeInitializer(int capacity) {
        this.holders = new Long2ObjectLinkedOpenHashMap<>(capacity);
    }

    @Override
    public int size() {
        return holders.size();
    }

    @Override
    public boolean isEmpty() {
        return holders.isEmpty();
    }

    @Override
    public long get(long expires, InvokedFeedback<V> feedback) {
        if (null == feedback) {
            return 0L;
        }

        long nextRequestId = nextRequestId();
        holders.put(nextRequestId, Holder.newHolder(expires, feedback));
        return nextRequestId;
    }

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

    private long nextRequestId() {
        return ++requestId == 0 ? ++requestId : requestId;
    }

    private static final class Holder {
        private static final Recycler<Holder> RECYCLER = new Recycler<>() {
            @Override
            protected Holder newObject(Handle<Holder> handle) {
                return new Holder(handle);
            }
        };

        private final Recycler.Handle<Holder> handle;
        private long expires;
        private InvokedFeedback<?> feedback;

        public Holder(Recycler.Handle<Holder> handle) {
            this.handle = handle;
        }

        private static Holder newHolder(long expires, InvokedFeedback<?> feedback) {
            Holder instance = RECYCLER.get();
            instance.expires = expires;
            instance.feedback = feedback;
            return instance;
        }

        private void recycle() {
            this.feedback = null;
            handle.recycle(this);
        }

        private boolean isValid() {
            return null != feedback && !feedback.isCompleted();
        }
    }
}
