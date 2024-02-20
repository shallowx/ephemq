package org.meteor.remote.invoke;

import java.util.function.Consumer;

public interface CallableSafeInitializer<V> {
    int size();

    boolean isEmpty();

    long get(long expires, InvokedFeedback<V> rejoin);

    boolean release(long rejoin, Consumer<InvokedFeedback<V>> consumer);

    int releaseExpired(Consumer<InvokedFeedback<V>> consumer);

    int releaseAll(Consumer<InvokedFeedback<V>> consumer);
}
