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

/**
 * The {@code InternalClient} class extends the {@code Client} class and provides
 * specialized behavior for internal clients. This class is not intended for
 * operations such as creating or deleting topics.
 * <p>
 * The class utilizes an internal logger to log significant events and errors.
 * It also provides methods to bind the client to a {@code MeterRegistry} for
 * monitoring purposes.
 */
public class InternalClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(InternalClient.class);
    /**
     * Holds an instance of CommonConfig which provides configuration settings
     * for the internal client operations. This configuration is used to manage
     * various parameters required for the client's interaction with the server.
     */
    private final CommonConfig configuration;

    /**
     * Constructs a new InternalClient with the specified name, client configuration, listener,
     * and common configuration.
     *
     * @param name the name of the internal client
     * @param clientConfig the configuration specific to the client
     * @param listener the listener for handling various client events
     * @param configuration the common configuration to be used by the client
     */
    public InternalClient(String name, ClientConfig clientConfig, CombineListener listener,
                          CommonConfig configuration) {
        super(name, clientConfig, listener);
        this.configuration = configuration;
    }

    /**
     * Creates a new client channel for communication.
     *
     * @param clientConfig The configuration for the client.
     * @param channel The Netty channel for network communication.
     * @param address The socket address to connect to.
     * @return A new instance of {@code ClientChannel} configured with the specified parameters.
     */
    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new InternalClientChannel(clientConfig, channel, address, configuration);
    }

    /**
     * Creates a new topic with the specified name, number of partitions, and replication factor.
     *
     * @param topic the name of the topic to be created.
     * @param partitions the number of partitions for the topic.
     * @param replicas the number of replicas for the topic.
     * @return a CreateTopicResponse object containing the response from the create topic operation.
     * @throws Exception if the operation is not supported or fails.
     */
    @Override
    public CreateTopicResponse createTopic(String topic, int partitions, int replicas) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[create topic]");
    }

    /**
     * Creates a topic with the specified name, number of partitions, and replicas, and with the specified configuration.
     *
     * @param topic the name of the topic to be created
     * @param partitions the number of partitions for the topic
     * @param replicas the number of replicas for the partitions
     * @param topicConfig the configuration settings for the topic
     * @return CreateTopicResponse containing the result of the topic creation operation
     * @throws Exception if an error occurs during the topic creation
     */
    @Override
    public CreateTopicResponse createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[create topic]");
    }

    /**
     * Deletes a specified topic from the internal client.
     *
     * @param topic the name of the topic to be deleted.
     * @return DeleteTopicResponse containing the result of the delete operation.
     * @throws Exception if the delete operation is not supported.
     */
    @Override
    public DeleteTopicResponse deleteTopic(String topic) throws Exception {
        throw new UnsupportedOperationException("Internal client not support operation[delete topic]");
    }

    /**
     * Binds the current client instance to the provided MeterRegistry for metrics monitoring.
     *
     * @param meterRegistry the MeterRegistry to which metrics will be registered, must not be null.
     */
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
