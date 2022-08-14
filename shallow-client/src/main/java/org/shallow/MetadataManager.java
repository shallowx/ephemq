package org.shallow;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.PartitionRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.server.CreateTopicRequest;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicRequest;
import org.shallow.proto.server.DelTopicResponse;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class MetadataManager implements ProcessCommand.Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataManager.class);

    private final ShallowChannelPool pool;
    private final ClientConfig config;
    private final LoadingCache<String, PartitionRecord> topics;
    private final LoadingCache<String, NodeRecord> clusters;
    private final EventExecutor scheduledMetadataTask;

    public MetadataManager(ClientConfig config) {
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.config = config;

        this.topics = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable PartitionRecord load(String key) throws Exception {
                return null;
            }
        });

        this.clusters = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable NodeRecord load(String key) throws Exception {
                return null;
            }
        });
        this.scheduledMetadataTask = newEventExecutorGroup(1, "metadata-task").next();
    }

    public void start() throws Exception{
        scheduledMetadataTask.scheduleAtFixedRate(this::refreshMetadata, 0,
                config.getRefreshMetadataIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public Promise<CreateTopicResponse> createTopic(byte command, String topic, int partitions, int latency) {
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setLatencies(latency)
                .build();

        Promise<CreateTopicResponse> promise = newImmediatePromise();
        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker().invoke(command, config.getInvokeExpiredMs(), promise, request, CreateTopicResponse.class);

        return promise;
    }

    public Promise<DelTopicResponse> delTopic(byte command, String topic) {
        DelTopicRequest request = DelTopicRequest.newBuilder().setTopic(topic).build();

        Promise<DelTopicResponse> promise = newImmediatePromise();

        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker().invoke(command, config.getInvokeExpiredMs(), promise, request, DelTopicResponse.class);

        return promise;
    }

    public void queryTopicRecord() {

    }

    public void queryNodeRecord() {

    }

    private void refreshMetadata() {

    }
}
