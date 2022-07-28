package org.shallow.api;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.List;

public abstract class AbstractMetadataProvider<T> {

    private final MetadataAPI api;
    public final LoadingCache<String, T> metadataCache;

    public AbstractMetadataProvider(MetadataAPI api, long expired) {
        this.api = api;
        this.metadataCache = Caffeine.newBuilder().refreshAfterWrite(Duration.ofSeconds(expired)).build(new CacheLoader<>() {
            @Override
            public @Nullable T load(String key) throws Exception {
                return AbstractMetadataProvider.this.load(key);
            }
        });
    }

    protected void put(String key, T v) {
        metadataCache.put(key, v);
    }

    protected void invalidate(String key) {
        metadataCache.invalidate(key);
    }

    protected T get(String key) {
        return metadataCache.get(key);
    }

    protected List<T> getWhole() {
        return metadataCache.asMap().values().stream().toList();
    }

    protected Promise<Boolean> newPromise() {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }

    protected void doModify(final String path, final List<T> content, final Promise<Boolean> promise, MappedFileConstants.Type type) {
        api.modify(path, switchMetadata(content), promise, type);
    }

    public void shutdownGracefully() {
        metadataCache.cleanUp();
    }

    protected abstract T load(String key);

    protected abstract String switchMetadata(List<T> t);

    protected abstract Future<Boolean> append(T t);

    protected abstract Future<Boolean> delete(T t);

    protected abstract T acquire(String key);
}
