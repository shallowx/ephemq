package org.meteor.remote.invoke;

import java.util.function.Consumer;

public interface InvokeHolder<V> {
    int size();

    boolean isEmpty();

    int hold(long expires, InvokeAnswer<V> rejoin);

    boolean free(int rejoin, Consumer<InvokeAnswer<V>> consumer);

    int freeExpired(Consumer<InvokeAnswer<V>> consumer);

    int freeEntire(Consumer<InvokeAnswer<V>> consumer);
}
