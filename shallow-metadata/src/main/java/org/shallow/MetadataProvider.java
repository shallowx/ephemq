package org.shallow;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public interface MetadataProvider<T> {

    Future<Boolean> append(T t);

    Future<Boolean> delete(T t);

    T acquire(String key);
}
