package org.meteor.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.ClientConfig;
import org.meteor.client.internal.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.TopicConfig;
import org.meteor.config.CommonConfig;
import org.meteor.coordinator.Coordinator;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicResponse;

import javax.annotation.Nonnull;
import java.net.SocketAddress;

import static org.meteor.metrics.config.MetricsConstants.*;

public class InternalClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalClient.class);
    private final CommonConfig configuration;
    private final Coordinator coordinator;

    public InternalClient(String name, ClientConfig clientConfig, CombineListener listener, CommonConfig configuration, Coordinator coordinator) {
        super(name, clientConfig, listener);
        this.configuration = configuration;
        this.coordinator = coordinator;
    }

    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InternalClientChannel(clientConfig, channel, address, configuration, coordinator);
    }

    @Override
    public CreateTopicResponse createTopic(String topic, int partitions, int replicas) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[create topic]");
    }

    @Override
    public CreateTopicResponse createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[create topic]");
    }

    @Override
    public DeleteTopicResponse deleteTopic(String topic) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[delete topic]");
    }

    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) refreshMetadataExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, configuration.getClusterName())
                    .tag(BROKER_TAG, configuration.getServerId())
                    .tag(TYPE_TAG, "client-task")
                    .tag(NAME, name)
                    .tag(ID, executor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : workerGroup) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, configuration.getClusterName())
                        .tag(BROKER_TAG, configuration.getServerId())
                        .tag(TYPE_TAG, "client-task")
                        .tag(NAME, name)
                        .tag(ID, executor.threadProperties().name())
                        .register(meterRegistry);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Inner client bind failed, executor[{}]", eventExecutor.toString(), t);
                }
            }
        }

    }
}
