package org.shallow.invoke;

public interface InvokeAnswer<V> {

    String SUCCESS = "SUCCESS";
    String FAILURE = "FAILURE";

    boolean isCompleted();

    boolean success(V v);

    boolean failure(Throwable cause);
}
