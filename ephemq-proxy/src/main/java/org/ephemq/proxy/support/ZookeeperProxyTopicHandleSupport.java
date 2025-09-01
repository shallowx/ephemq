package org.ephemq.proxy.support;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.proxy.core.ProxyConfig;
import org.ephemq.remote.proto.TopicInfo;
import org.ephemq.remote.proto.server.QueryTopicInfoRequest;
import org.ephemq.remote.proto.server.QueryTopicInfoResponse;
import org.ephemq.support.Manager;
import org.ephemq.ledger.ParticipantSupport;
import org.ephemq.zookeeper.ZookeeperTopicHandleSupport;

/**
 * ZookeeperProxyTopicHandleSupport is a class that extends ZookeeperTopicHandleSupport and implements
 * ProxyTopicHandleSupport to provide topic handling capabilities with ZooKeeper integration in a proxy
 * environment. This class is responsible for managing the metadata for topics, fetching topic information
 * from upstream if necessary, and ensuring consistency of topic metadata.
 */
class ZookeeperProxyTopicHandleSupport extends ZookeeperTopicHandleSupport implements ProxyTopicHandleSupport {
    private static final InternalLogger logger =
            InternalLoggerFactory.getLogger(ZookeeperProxyTopicHandleSupport.class);
    /**
     * Provides support functionality for Ledger synchronization within
     * the context of Zookeeper proxy topic handling.
     */
    private final LedgerSyncSupport syncSupport;
    /**
     * Holds the configuration settings for the proxy connection.
     */
    private final ProxyConfig proxyConfiguration;
    /**
     * A cache holding the metadata information of topics, where keys are topic names and values are {@link TopicInfo} objects.
     * The cache automatically loads data using a specified loading mechanism when a cache miss occurs.
     */
    private LoadingCache<String, TopicInfo> topicMetaLoadingCache;

    /**
     * Constructs a new ZookeeperProxyTopicHandleSupport instance.
     *
     * @param config  the configuration for the proxy
     * @param manager the manager responsible for handling the operations
     */
    public ZookeeperProxyTopicHandleSupport(ProxyConfig config, Manager manager) {
        super();
        this.proxyConfiguration = config;
        this.manager = manager;
        this.syncSupport = ((ProxyDefaultManager) manager).getLedgerSyncSupport();
        this.participantSupport = new ParticipantSupport(manager);
    }

    /**
     * Initializes and starts the topic metadata loading cache.
     * The cache is configured to refresh its entries every 30 seconds.
     * The loading mechanism retrieves metadata for topics from the upstream
     * source and handles exceptions that might occur during the process.
     *
     * @throws Exception if an error occurs while initializing the cache or loading topic metadata
     */
    @Override
    public void start() throws Exception {
        this.topicMetaLoadingCache = Caffeine.newBuilder().refreshAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable TopicInfo load(String key) throws Exception {
                        try {
                            ClientChannel channel = syncSupport.getProxyClient().getActiveChannel(null);
                            Map<String, TopicInfo> topicInfos = getFromUpstream(Lists.newArrayList(key), channel);
                            return (topicInfos == null || topicInfos.isEmpty()) ? null : topicInfos.get(key);
                        } catch (Exception e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Get topic[{}] info form upstream failed, ", key, e.getMessage(), e);
                            }
                            return null;
                        }
                    }
                });
    }

    /**
     * Requests topic information from an upstream service.
     *
     * @param topics a list of topic names for which information is requested
     * @param channel the client channel used to communicate with the upstream service
     * @return a map where the keys are topic names and the values are topic information; returns null if there is an error retrieving the information
     */
    private Map<String, TopicInfo> getFromUpstream(List<String> topics, ClientChannel channel) {
        Promise<QueryTopicInfoResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        QueryTopicInfoRequest.Builder builder = QueryTopicInfoRequest.newBuilder();
        if (topics != null && !topics.isEmpty()) {
            builder.addAllTopicNames(topics);
        }

        try {
            channel.invoker().queryTopicInfo(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds(), promise, builder.build());
            QueryTopicInfoResponse response = promise.get(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
            return response.getTopicInfosMap();
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
            return null;
        }
    }

    /**
     * Retrieves metadata for a list of topics.
     *
     * @param topics a list of topic names for which metadata is to be fetched
     * @return a map where keys are topic names and values are TopicInfo objects containing metadata
     */
    @Override
    public Map<String, TopicInfo> getTopicMetadata(List<String> topics) {
        if (topics.isEmpty()) {
            return getFromUpstream(topics, null);
        }
        Map<String, TopicInfo> ret = new Object2ObjectOpenHashMap<>();
        for (String topic : topics) {
            TopicInfo info = topicMetaLoadingCache.get(topic);
            if (info == null) {
                continue;
            }
            ret.put(topic, info);
        }
        return ret;
    }

    /**
     * Refreshes the metadata of the specified topics using information from the upstream.
     * Updates the local cache with the new metadata.
     *
     * @param topics The list of topics for which metadata needs to be refreshed.
     * @param channel The ClientChannel instance used to fetch the metadata from upstream.
     */
    @Override
    public void refreshTopicMetadata(List<String> topics, ClientChannel channel) {
        Map<String, TopicInfo> topicInfos = getFromUpstream(topics, channel);
        if (topicInfos == null) {
            for (String topic : topics) {
                invalidTopicMetadata(topic);
            }
            return;
        }

        for (Map.Entry<String, TopicInfo> entry : topicInfos.entrySet()) {
            String topic = entry.getKey();
            TopicInfo info = entry.getValue();
            if (info == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Topic[{}] info is NULL", topic);
                }
                continue;
            }
            topicMetaLoadingCache.put(topic, info);
        }
    }

    /**
     * Invalidates the metadata cache entry for a specific topic.
     *
     * @param topic the name of the topic whose metadata cache entry should be invalidated
     */
    @Override
    public void invalidTopicMetadata(String topic) {
        topicMetaLoadingCache.invalidate(topic);
    }
}
