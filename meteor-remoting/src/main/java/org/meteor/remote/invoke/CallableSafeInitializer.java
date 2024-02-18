package org.meteor.remote.invoke;

import java.util.function.Consumer;

public interface CallableSafeInitializer<V> {
    int size();

    boolean isEmpty();

    int get(long expires, InvokedFeedback<V> rejoin);

    boolean free(int rejoin, Consumer<InvokedFeedback<V>> consumer);

    int freeExpired(Consumer<InvokedFeedback<V>> consumer);

    int freeEntire(Consumer<InvokedFeedback<V>> consumer);
}
