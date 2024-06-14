package org.meteor.internal;

import static org.meteor.client.util.MessageConstants.CLIENT_NETTY_PENDING_TASK_NAME;
import static org.meteor.metrics.config.MetricsConstants.BROKER_TAG;
import static org.meteor.metrics.config.MetricsConstants.CLUSTER_TAG;
import static org.meteor.metrics.config.MetricsConstants.ID;
import static org.meteor.metrics.config.MetricsConstants.NAME;
import static org.meteor.metrics.config.MetricsConstants.TYPE_TAG;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.net.SocketAddress;
import javax.annotation.Nonnull;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.ClientConfig;
import org.meteor.client.core.CombineListener;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.TopicConfig;
import org.meteor.config.CommonConfig;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicResponse;

public class InternalClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalClient.class);
    private final CommonConfig configuration;

    public InternalClient(String name, ClientConfig clientConfig, CombineListener listener,
                          CommonConfig configuration) {
        super(name, clientConfig, listener);
        this.configuration = configuration;
    }

    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InternalClientChannel(clientConfig, channel, address, configuration);
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
