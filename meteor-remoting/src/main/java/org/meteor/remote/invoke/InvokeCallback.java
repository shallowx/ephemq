package org.meteor.remote.invoke;

@FunctionalInterface
public interface InvokeCallback<V> {
    void operationCompleted(V v, Throwable cause);
}
