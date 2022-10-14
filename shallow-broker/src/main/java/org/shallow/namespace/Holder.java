package org.shallow.namespace;

public interface Holder<T> {

    T get(String clusterName);
}
