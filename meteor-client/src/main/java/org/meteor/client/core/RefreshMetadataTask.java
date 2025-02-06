package org.meteor.client.core;

import io.netty.util.concurrent.Future;
import org.meteor.client.exception.ClientRefreshException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.TopicInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Task for refreshing metadata in the client periodically.
 * <p>
 * This task ensures that the metadata related to topics and message
 * routing is updated at regular intervals as specified in the client
 * configuration. It handles any exceptions that occur during the
 * refresh process and reschedules itself as long as the client is
 * not shutting down.
 */
final class RefreshMetadataTask implements Runnable {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RefreshMetadataTask.class);
    /**
     * Configuration settings for the client used by the RefreshMetadataTask.
     */
    private final ClientConfig config;
    /**
     * Client instance used for performing operations related to refreshing metadata.
     * This client is provided during the creation of the RefreshMetadataTask and remains constant.
     */
    private final Client client;

    /**
     * Constructs a new instance of RefreshMetadataTask with the specified client and configuration.
     *
     * @param client the client instance used for the task
     * @param config the client configuration for the task
     */
    public RefreshMetadataTask(Client client, ClientConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Executes the metadata refresh task. This method checks if the metadata refresh executor
     * is shutting down before proceeding. If not, it attempts to refresh the metadata by
     * calling the `refreshMetadata` method. Any errors encountered during the refresh are logged.
     * After executing the metadata refresh, if the executor is still not shutting down,
     * the task is rescheduled to run again after a delay specified by the metadata refresh period
     * configuration.
     */
    @Override
    public void run() {
        if (client.refreshMetadataExecutor.isShuttingDown()) {
            return;
        }

        try {
            refreshMetadata();
        } catch (Throwable t) {
            String message = t.getMessage();
            if (message == null || message.isEmpty()) {
                message = t.getClass().getName();
            }

            if (logger.isErrorEnabled()) {
                logger.error("Refresh metadata failed", message);
            }
        }

        if (!client.refreshMetadataExecutor.isShuttingDown()) {
            client.refreshMetadataExecutor.schedule(this, config.getMetadataRefreshPeriodMilliseconds(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Refreshes the metadata for message routing by querying the cluster and topic information,
     * and updating the client's router cache accordingly.
     * <p>
     * This method performs the following steps:
     * 1. Collects topics that require metadata refresh.
     * 2. Queries the cluster information.
     * 3. Fetches topic information for the collected topics.
     * 4. Builds new message routers for the topics based on the fetched information.
     * 5. Updates the client's router cache with the newly built routers.
     * <p>
     * If the queried router information is empty, the method logs a debug message and exits.
     * <p>
     * If an error occurs during the metadata refresh, it is logged as an error.
     */
    private void refreshMetadata() {
        Set<String> topics = new HashSet<>();
        for (Map.Entry<String, Future<MessageRouter>> entry : client.getRouters().entrySet()) {
            String topic = entry.getKey();
            Future<MessageRouter> future = entry.getValue();
            if (future.isDone() && future.isSuccess() && future.getNow() == null) {
                client.getRouters().remove(topic, future);
                continue;
            }

            topics.add(topic);
        }

        if (topics.isEmpty()) {
            return;
        }

        Map<String, MessageRouter> queryRouters = new HashMap<>();
        try {
            ClientChannel channel = client.getActiveChannel(null);
            ClusterInfo clusterInfo = client.queryClusterInfo(channel);
            if (clusterInfo == null) {
                throw new ClientRefreshException("Cluster node not found");
            }

            Map<String, TopicInfo> topicInfos = client.queryTopicInfos(channel, topics.toArray(new String[0]));
            for (String topic : topics) {
                TopicInfo topicInfo = topicInfos.get(topic);
                if (topicInfo == null) {
                    continue;
                }

                MessageRouter router = client.buildRouter(topic, clusterInfo, topicInfo);
                if (router == null) {
                    continue;
                }
                queryRouters.put(topic, router);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }

        if (queryRouters.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Message router is empty");
            }
            return;
        }

        for (Map.Entry<String, MessageRouter> entry : queryRouters.entrySet()) {
            client.cachingRouter(entry.getKey(), entry.getValue());
        }
    }
}
