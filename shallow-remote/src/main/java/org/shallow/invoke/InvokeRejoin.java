package org.shallow.invoke;

public interface InvokeRejoin<V> {

    boolean isCompleted();

    boolean success(V v);

    boolean failure(Throwable cause);
}
