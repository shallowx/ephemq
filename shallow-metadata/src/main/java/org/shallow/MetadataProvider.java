package org.shallow;

import io.netty.util.concurrent.Future;
public interface MetadataProvider<T> {

    Future<Boolean> append(T t);

    Future<Boolean> delete(T t);

    T acquire(String key);
}
