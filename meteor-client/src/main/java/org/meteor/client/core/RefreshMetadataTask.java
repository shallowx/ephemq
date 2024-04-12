package org.meteor.client.core;

import io.netty.util.concurrent.Future;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.meteor.client.exception.ClientRefreshException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.TopicInfo;

final class RefreshMetadataTask implements Runnable {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RefreshMetadataTask.class);
    private final ClientConfig config;
    private final Client client;

    public RefreshMetadataTask(Client client, ClientConfig config) {
        this.client = client;
        this.config = config;
    }

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
