package org.shallow.invoke;

@FunctionalInterface
public interface Callback<V> {
    void operationCompleted(V v, Throwable cause);
}
