package org.shallow.invoke;

import io.netty.util.Recycler;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.function.Consumer;

import static org.shallow.ObjectUtil.isNotNull;
import static org.shallow.ObjectUtil.isNull;

public class GenericInvokeHolder<V> implements InvokeHolder<V> {

    private final Int2ObjectMap<Holder> holders;
    private int rejoin;

    public GenericInvokeHolder() {
        this(2048);
    }

    public GenericInvokeHolder(int capacity) {
        this.holders = new Int2ObjectLinkedOpenHashMap<>(capacity);
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
    public int hold(long expires, InvokeRejoin<V> rejoin) {
        if (isNull(rejoin)) {
            return 0;
        }

        int nextRejoin = nextRejoin();
        holders.put(nextRejoin, Holder.newHolder(expires, rejoin));
        return nextRejoin;
    }

    @Override
    public boolean consume(int rejoin, Consumer<InvokeRejoin<V>> consumer) {
        if (rejoin == 0) {
            return false;
        }

        Holder holder = holders.get(rejoin);
        if (isNull(holder)) {
            return false;
        }

        if (holder.isValid() && isNotNull(consumer)) {
            doConsume(holder, consumer);
        }
        holder.recycle();
        return true;
    }

    @Override
    public int consumeWholeVerbExpired(Consumer<InvokeRejoin<V>> consumer, Long expired) {
        if (isEmpty()) {
            return 0;
        }

        int whole = 0;
        ObjectIterator<Int2ObjectMap.Entry<Holder>> iterator = holders.int2ObjectEntrySet().iterator();
        while(iterator.hasNext()) {
            Holder holder = iterator.next().getValue();
            boolean valid = holder.isValid();

            if (isNotNull(expired) && valid && holder.expires > expired) {
                continue;
            }

            if (valid && isNotNull(consumer)) {
                doConsume(holder, consumer);
            }

            iterator.remove();
            holder.recycle();
            whole++;
        }
        return whole;
    }

    private void doConsume(Holder holder, Consumer<InvokeRejoin<V>> consumer) {
        @SuppressWarnings("unchecked")
        InvokeRejoin<V> rejoin = (InvokeRejoin<V>) holder.rejoin;
        try {
            consumer.accept(rejoin);
        } catch (Throwable cause) {
            rejoin.failure(cause);
        }
    }

    private int nextRejoin() {
        return ++rejoin == 0 ? ++rejoin : rejoin;
    }

    private static final class Holder {
        private static final Recycler<Holder> RECYCLER = new Recycler<>() {
            @Override
            protected Holder newObject(Handle<Holder> handle) {
                return new Holder(handle);
            }
        };

        private long expires;
        private InvokeRejoin<?> rejoin;
        private final Recycler.Handle<Holder> handle;

        public Holder(Recycler.Handle<Holder> handle) {
            this.handle = handle;
        }

        private static Holder newHolder(long expires, InvokeRejoin<?> rejoin) {
            Holder instance = RECYCLER.get();
            instance.expires = expires;
            instance.rejoin = rejoin;
            return instance;
        }

        private void recycle() {
            this.rejoin = null;
            handle.recycle(this);
        }

        private boolean isValid() {
            return isNotNull(rejoin) && !rejoin.isCompleted();
        }
    }
}
