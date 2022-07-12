package org.shallow.invoke;

@FunctionalInterface
public interface Invoker<V> {
    void onCompleted(V v, Throwable cause);
}
