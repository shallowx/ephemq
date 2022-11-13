package org.leopard.remote.invoke;

import io.netty.util.Recycler;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.function.Consumer;

public final class GenericInvokeHolder<V> implements InvokeHolder<V> {

    private final Int2ObjectMap<Holder> holders;
    private int offset;

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
    public int hold(long expires, InvokeAnswer<V> answer) {
        if (null == answer) {
            return 0;
        }

        var nextOffset = nextOffset();
        holders.put(nextOffset, Holder.newHolder(expires, answer));
        return nextOffset;
    }

    @Override
    public boolean free(int answer, Consumer<InvokeAnswer<V>> consumer) {
        if (answer == 0) {
            return false;
        }

        Holder holder = holders.remove(answer);
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
    public int freeEntire(Consumer<InvokeAnswer<V>> consumer) {
        if (isEmpty()) {
            return 0;
        }

        var whole = 0;
        ObjectIterator<Int2ObjectMap.Entry<Holder>> iterator = holders.int2ObjectEntrySet().iterator();
        while(iterator.hasNext()) {
            Holder holder = iterator.next().getValue();
            if (holder.isValid() && null != consumer) {
                doConsume(holder, consumer);
            }

            iterator.remove();
            holder.recycle();
            whole++;
        }
        return whole;
    }


    @Override
    public int freeExpired(Consumer<InvokeAnswer<V>> consumer) {
        if (isEmpty()) {
            return 0;
        }

        long now = System.currentTimeMillis();

        var whole = 0;
        ObjectIterator<Int2ObjectMap.Entry<Holder>> iterator = holders.int2ObjectEntrySet().iterator();
        while(iterator.hasNext()) {
            Holder holder = iterator.next().getValue();
            boolean valid = holder.isValid();

            if (valid && holder.expired > now) {
                continue;
            }

            if (valid && null != consumer) {
                doConsume(holder, consumer);
            }

            iterator.remove();
            holder.recycle();
            whole++;
        }
        return whole;
    }

    private void doConsume(Holder holder, Consumer<InvokeAnswer<V>> consumer) {
        @SuppressWarnings("unchecked")
        InvokeAnswer<V> answer = (GenericInvokeAnswer<V>) holder.answer;
        try {
            consumer.accept(answer);
        } catch (Throwable cause) {
            answer.failure(cause);
        }
    }

    private int nextOffset() {
        return ++offset == 0 ? ++offset : offset;
    }

    private static final class Holder {
        private static final Recycler<Holder> RECYCLER = new Recycler<>() {
            @Override
            protected Holder newObject(Handle<Holder> handle) {
                return new Holder(handle);
            }
        };

        private long expired;
        private InvokeAnswer<?> answer;
        private final Recycler.Handle<Holder> handle;

        public Holder(Recycler.Handle<Holder> handle) {
            this.handle = handle;
        }

        private static Holder newHolder(long expired, InvokeAnswer<?> answer) {
            Holder instance = RECYCLER.get();
            instance.expired = expired;
            instance.answer = answer;
            return instance;
        }

        private void recycle() {
            this.answer = null;
            handle.recycle(this);
        }

        private boolean isValid() {
            return null != answer && !answer.isCompleted();
        }
    }
}
