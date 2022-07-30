package org.shallow.topic;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.api.MappedFileConstants;
import org.shallow.api.MetaMappedFileAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionInfo;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;
import org.shallow.util.Ack;
import org.shallow.util.JsonUtil;
import org.shallow.util.NetworkUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.shallow.api.MappedFileConstants.TOPICS;
import static org.shallow.api.MappedFileConstants.Type.APPEND;
import static org.shallow.api.MappedFileConstants.Type.DELETE;
import static org.shallow.util.Ack.SUCCESS;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class TopicMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MetaMappedFileAPI api;
    private final LoadingCache<String, List<PartitionInfo>> topicsInfoCache;

    public TopicMetadataProvider(MetaMappedFileAPI api, EventExecutor cacheExecutor, EventExecutor apiExecutor, long expired) {
        this.api = api;
        this.cacheExecutor = cacheExecutor;
        this.apiExecutor = apiExecutor;

        this.topicsInfoCache = Caffeine.newBuilder()
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable List<PartitionInfo> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    public void write2Cache(String topic, int partitions, int latency, Promise<MessageLite> promise) {
        try {
            if (cacheExecutor.inEventLoop()) {
                doWrite2Cache(topic, partitions, latency, promise);
            } else {
                cacheExecutor.execute(() -> doWrite2Cache(topic, partitions, latency, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void doWrite2Cache(String topic, int partitions, int latency, Promise<MessageLite> promise) {
        try {
            topicsInfoCache.put(topic, assemblePartitions(topic, partitions, latency));
            final Map<String, List<PartitionInfo>> topicInfoMeta = getAllTopics();
            final String content = JsonUtil.object2Json(topicInfoMeta);

            Promise<Boolean> modifyPromise = newImmediatePromise();
            modifyPromise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                if (f.isSuccess()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[doWrite2Cache] - write content to file successfully, content<{}>", content);
                    }
                } else {
                    api.modify(TOPICS, content, APPEND, null);
                }
            });

            if (apiExecutor.inEventLoop()) {
                api.modify(TOPICS, content, APPEND, modifyPromise);
            } else {
                apiExecutor.execute(() -> api.modify(TOPICS, content, APPEND, modifyPromise));
            }

            promise.trySuccess(CreateTopicResponse.newBuilder()
                    .setTopic(topic)
                    .setAck(SUCCESS)
                    .setLatency(latency)
                    .setPartitions(partitions)
                    .build());
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[doWrite2Cache] - Failed to write content to file, content<{}>, cause:{}", topic, t);
            }
            promise.tryFailure(t);
        }
    }

    private List<PartitionInfo> assemblePartitions(String topic, int partitions, int latency) {
        return List.of(new PartitionInfo(0, 1, 1, "node1", null, null, null));
    }

    public void delFromCache(String topic, Promise<MessageLite> promise) {
        try {
            if (cacheExecutor.inEventLoop()) {
                doDelFromCache(topic, promise);
            } else {
                cacheExecutor.execute(() -> doDelFromCache(topic, promise));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[delFromCache] - Failed to del topic from cache, content<{}>", topic);
            }
            promise.tryFailure(t);
        }
    }

    private void doDelFromCache(String topic, Promise<MessageLite> promise) {
        try {
            topicsInfoCache.invalidate(topic);
            getAllTopics().entrySet().removeIf(k -> k.getKey().equals(topic));
            final String content = JsonUtil.object2Json(getAllTopics());

            final Promise<Boolean> modifyPromise = newImmediatePromise();
            modifyPromise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                if (f.isSuccess()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[doDelFromCache] - del topic<{}> from file successfully", topic);
                    }
                } else {
                    api.modify(TOPICS, content, DELETE, null);
                }
            });

            if (apiExecutor.inEventLoop()) {
                api.modify(TOPICS, content, DELETE, modifyPromise);
            } else {
                apiExecutor.execute(() -> api.modify(TOPICS, content, DELETE, modifyPromise));
            }

            promise.trySuccess(DelTopicResponse.newBuilder().build());
        } catch (Throwable t)  {
            if (logger.isErrorEnabled()) {
                logger.error("[delFromCache] - Failed to del topic from file, topic<{}>", topic);
            }
            promise.tryFailure(t);
        }
    }

    public Map<String, List<PartitionInfo>> getAllTopics() {
        return topicsInfoCache.asMap();
    }

    public List<PartitionInfo> getTopicInfo(String topic) {
        return topicsInfoCache.get(topic);
    }
}
