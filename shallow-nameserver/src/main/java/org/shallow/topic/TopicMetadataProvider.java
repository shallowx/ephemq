package org.shallow.topic;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.api.AbstractMetadataProvider;
import org.shallow.api.MappedFileConstants;
import org.shallow.api.MetadataAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.List;

import static org.shallow.JsonUtil.object2Json;
import static org.shallow.api.MappedFileConstants.TOPICS;

public class TopicMetadataProvider extends AbstractMetadataProvider<TopicMetadata> {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicMetadataProvider.class);

    private final EventExecutor write2FileExecutor;
    private final MetadataAPI api;

    public TopicMetadataProvider(MetadataAPI api, EventExecutor executor, long expired) {
        super(api, expired);
        this.api = api;
        this.write2FileExecutor = executor;
    }

    @Override
    public TopicMetadata acquire(String key) {
        return get(key);
    }

    @Override
    public Future<Boolean> append(TopicMetadata topicMetadata) {
        Promise<Boolean> promise = newPromise();
        try {
            final String path = api.assemblesPath(TOPICS);
            if (write2FileExecutor.inEventLoop()) {
                doAppend(path, topicMetadata, promise);
            } else {
                write2FileExecutor.execute(() -> doAppend(path, topicMetadata, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    @Override
    public Future<Boolean> delete(TopicMetadata topicMetadata) {
        Promise<Boolean> promise = newPromise();
        try {
            final String path = api.assemblesPath(TOPICS);
            if (write2FileExecutor.inEventLoop()) {
                doDelete(path, topicMetadata, promise);
            } else {
                write2FileExecutor.execute(() -> doDelete(path, topicMetadata, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    private void doAppend(String path, TopicMetadata topicMetadata, Promise<Boolean> promise) {
        final String topic = topicMetadata.name();
        put(topic, topicMetadata);

        List<TopicMetadata> topics = getWhole();
        doModify(path, topics, promise, MappedFileConstants.Type.APPEND);
    }

    private void doDelete(String path, TopicMetadata topicMetadata, Promise<Boolean> promise) {
        invalidate(topicMetadata.name());

        List<TopicMetadata> topics = getWhole();
        topics.removeIf(f -> f.name().equals(topicMetadata.name()));

        doModify(path, topics, promise, MappedFileConstants.Type.DELETE);
    }

    @Override
    protected String switchMetadata(List<TopicMetadata> topics) {
        return object2Json(topics);
    }

    @Override
    protected TopicMetadata load(String key) {
        return null;
    }
}
