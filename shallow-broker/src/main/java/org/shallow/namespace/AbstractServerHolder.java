package org.shallow.namespace;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public abstract class AbstractServerHolder<T> implements Holder<T> {
      private final Map<String, T> clients = new ConcurrentHashMap<>();

    @Override
    public T get(String clusterName) {
        return clients.computeIfAbsent(clusterName, namespace -> {
            try {
                return createServer(namespace, () -> null);
            } catch (Exception e) {
                return null;
            }
        });
    }

    abstract T createServer(String clusterName, Supplier<T> supplier) throws Exception;
}
