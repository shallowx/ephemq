package org.leopard.remote.invoke;

public interface InvokeAnswer<V> {

    boolean isCompleted();

    boolean success(V v);

    boolean failure(Throwable cause);
}
