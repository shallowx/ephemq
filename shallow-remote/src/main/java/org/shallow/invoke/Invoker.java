package org.shallow.invoke;

@FunctionalInterface
public interface Invoker<V> {
    void operationCompleted(V v, Throwable cause);
}
