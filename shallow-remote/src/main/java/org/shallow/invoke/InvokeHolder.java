package org.shallow.invoke;

import java.util.function.Consumer;

public interface InvokeHolder<V> {
    int size();

    boolean isEmpty();

    int hold(long expires, InvokeRejoin<V> rejoin);

    boolean consume(int rejoin, Consumer<InvokeRejoin<V>> consumer);

    int consumeWholeVerbExpired(Consumer<InvokeRejoin<V>> consumer, Long expired);
}
