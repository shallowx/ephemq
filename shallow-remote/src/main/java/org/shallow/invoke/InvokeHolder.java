package org.shallow.invoke;

import java.util.function.Consumer;

public interface InvokeHolder<V> {
    int size();

    boolean isEmpty();

    int hold(long expires, InvokeAnswer<V> rejoin);

    boolean consume(int rejoin, Consumer<InvokeAnswer<V>> consumer);

    int consumeExpired(Consumer<InvokeAnswer<V>> consumer);
    int consumeWhole(Consumer<InvokeAnswer<V>> consumer);
}
