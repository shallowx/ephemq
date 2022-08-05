package org.shallow.provider;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.units.qual.A;
import org.shallow.api.MappedFileAPI;
import org.shallow.internal.MetadataConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.Partition;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;
import org.shallow.proto.server.QueryTopicInfoResponse;
import org.shallow.proto.server.RegisterNodeResponse;
import org.shallow.util.JsonUtil;
import org.shallow.util.NetworkUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.shallow.api.MappedFileConstants.TOPICS;
import static org.shallow.api.MappedFileConstants.Type.APPEND;
import static org.shallow.api.MappedFileConstants.Type.DELETE;
import static org.shallow.util.Ack.SUCCESS;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public class TopicMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MappedFileAPI api;
    private final LoadingCache<String, List<Partition>> topicsCache;

    public TopicMetadataProvider(MappedFileAPI api, EventExecutorGroup group) {
        this.api = api;
        this.cacheExecutor = group.next();
        this.apiExecutor = group.next();

        this.topicsCache = Caffeine.newBuilder()
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable List<Partition> load(String key) throws Exception {
                        return new CopyOnWriteArrayList<>();
                    }
                });
    }

    public void start() {
        this.populate();
        cacheExecutor.scheduleWithFixedDelay(this::scheduleWrite2File, 60000, 60000, TimeUnit.MILLISECONDS);
    }

    public void write2CacheAndFile(String topic, int partitions, int latency, Promise<CreateTopicResponse> promise) {
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

    private void doWrite2Cache(String topic, int partitions, int latency, Promise<CreateTopicResponse> promise) {
        try {
            topicsCache.put(topic, assemblePartitions(topic, partitions, latency));

            final Map<String, List<Partition>> topicInfoMeta = getAllTopics();
            final String topics = JsonUtil.object2Json(topicInfoMeta);

            if (apiExecutor.inEventLoop()) {
                api.modify(TOPICS, topics, APPEND);
            } else {
                apiExecutor.execute(() -> api.modify(TOPICS, topics, APPEND));
            }

            promise.trySuccess(CreateTopicResponse.newBuilder()
                    .setTopic(topic)
                    .setAck(SUCCESS)
                    .setLatency(latency)
                    .setPartitions(partitions)
                    .build());

        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[doWrite2Cache] - Failed to write topic info to file, content<{}>, cause:{}", topic, t);
            }
            promise.tryFailure(t);
        }
    }

    private List<Partition> assemblePartitions(String topic, int partitions, int latency) {
        return List.of(new Partition(0, 1, 1, "node1", null, null, null));
    }

    public void delFromCache(String topic, Promise<DelTopicResponse> promise) {
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

    private void doDelFromCache(String topic, Promise<DelTopicResponse> promise) {
        try {
            topicsCache.invalidate(topic);
            getAllTopics().entrySet().removeIf(k -> k.getKey().equals(topic));
            final String topics = JsonUtil.object2Json(getAllTopics());

            if (apiExecutor.inEventLoop()) {
                api.modify(TOPICS, topics, DELETE);
            } else {
                apiExecutor.execute(() -> api.modify(TOPICS, topics, DELETE));
            }

            promise.trySuccess(DelTopicResponse
                    .newBuilder()
                    .setTopic(topic)
                    .setAck(SUCCESS)
                    .build());
        } catch (Throwable t)  {
            if (logger.isErrorEnabled()) {
                logger.error("[delFromCache] - Failed to del topic from file, topic<{}>", topic);
            }
            promise.tryFailure(t);
        }
    }

    private void populate() {
        final String partitions = api.read(TOPICS);
        final Map<String, List<Partition>> topics = JsonUtil.json2Object(partitions,
                new TypeToken<Map<String, List<Partition>>>() {}.getType());
        if (isNotNull(topics) && !topics.isEmpty()) {
            topicsCache.putAll(topics);
        }
    }

    private void scheduleWrite2File() {
        final ConcurrentMap<String, List<Partition>> topics = topicsCache.asMap();
        if (topics.isEmpty()) {
            return;
        }
        final String content = JsonUtil.object2Json(topics);
        api.modify(TOPICS, content, APPEND);
    }

    public Map<String, List<Partition>> getAllTopics() {
        return topicsCache.asMap();
    }

    public List<Partition> getTopicInfo(String topic) {
        return topicsCache.get(topic);
    }

    // TODO
    public void queryTopicInfo(String topic, Promise<QueryTopicInfoResponse> promise) {
        QueryTopicInfoResponse.Builder builder = QueryTopicInfoResponse.newBuilder();
        promise.trySuccess(builder.build());
    }
}
