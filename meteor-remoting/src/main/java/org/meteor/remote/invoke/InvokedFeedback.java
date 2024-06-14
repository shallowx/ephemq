package org.meteor.remote.invoke;

public interface InvokedFeedback<V> {
    boolean isCompleted();
    boolean success(V v);
    boolean failure(Throwable cause);
}
