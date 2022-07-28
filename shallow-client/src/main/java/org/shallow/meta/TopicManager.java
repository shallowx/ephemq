package org.shallow.meta;

import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.ClientConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.server.CreateTopicRequest;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicRequest;
import org.shallow.proto.server.DelTopicResponse;

import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class TopicManager implements ProcessCommand.Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final ShallowChannelPool pool;
    private final ClientConfig config;

    public TopicManager(ClientConfig config) {
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.config = config;
    }

    public Promise<CreateTopicResponse> createTopic(String topic, int partitions, int latency) {
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setLatency(latency)
                .setPartitions(partitions)
                .build();

        Promise<CreateTopicResponse> promise = newImmediatePromise();

        ClientChannel channel = acquire();
        channel.invoker().invoke(ProcessCommand.Server.CREATE_TOPIC, config.getDefaultInvokeExpiredMs(), promise, request, CreateTopicResponse.class);

        return promise;
    }

    public Promise<DelTopicResponse> delTopic(String topic) {
        DelTopicRequest request = DelTopicRequest.newBuilder().setTopic(topic).build();

        Promise<DelTopicResponse> promise = newImmediatePromise();

        ClientChannel channel = acquire();
        channel.invoker().invoke(ProcessCommand.Server.DELETE_TOPIC, config.getDefaultInvokeExpiredMs(), promise, request, DelTopicResponse.class);

        return promise;
    }

    private ClientChannel acquire() {
        try {
            return pool.acquire().get(config.getConnectTimeOutMs(), TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw new RuntimeException("[Acquire] - failed to acquire random channel from pool", t);
        }
    }
}
