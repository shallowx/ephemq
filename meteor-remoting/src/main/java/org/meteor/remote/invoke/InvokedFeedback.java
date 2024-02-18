package org.meteor.remote.invoke;

@SuppressWarnings("all")
public interface InvokedFeedback<V> {

    boolean isCompleted();

    boolean success(V v);

    boolean failure(Throwable cause);
}
