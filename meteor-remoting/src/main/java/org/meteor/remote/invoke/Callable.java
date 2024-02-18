package org.meteor.remote.invoke;

@FunctionalInterface
public interface Callable<V> {
    void onCompleted(V v, Throwable cause);
}
