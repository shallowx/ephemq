package org.meteor.proxy.coordinatior;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.coordinatior.Coordinator;
import org.meteor.coordinatior.ParticipantCoordinator;
import org.meteor.coordinatior.ZookeeperTopicCoordinator;
import org.meteor.proxy.internal.ProxyConfig;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.server.QueryTopicInfoRequest;
import org.meteor.remote.proto.server.QueryTopicInfoResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ZookeeperProxyTopicCoordinator extends ZookeeperTopicCoordinator implements ProxyTopicCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyTopicCoordinator.class);
    private LoadingCache<String, TopicInfo> topicMetaLoadingCache;
    private final LedgerSyncCoordinator syncCoordinator;
    private final ProxyConfig proxyConfiguration;
    public ZookeeperProxyTopicCoordinator(ProxyConfig config, Coordinator coordinator) {
        this.proxyConfiguration = config;
        this.coordinator = coordinator;
        this.syncCoordinator = ((ProxyDefaultCoordinator) coordinator).getLedgerSyncCoordinator();
        this.participantCoordinator = new ParticipantCoordinator(coordinator);
    }

    @Override
    public void start() throws Exception {
        this.topicMetaLoadingCache = Caffeine.newBuilder().refreshAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable TopicInfo load(String key) throws Exception {
                        try {
                            ClientChannel channel = syncCoordinator.getProxyClient().fetchChannel(null);
                            Map<String, TopicInfo> ret = acquireFromUpstream(Lists.newArrayList(key), channel);
                            return (ret == null || ret.isEmpty()) ? null : ret.get(key);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                });
    }
    private Map<String, TopicInfo> acquireFromUpstream(List<String> topics, ClientChannel channel) {
        Promise<QueryTopicInfoResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        QueryTopicInfoRequest.Builder builder = QueryTopicInfoRequest.newBuilder();
        if (topics != null && !topics.isEmpty()) {
            builder.addAllTopicNames(topics);
        }

        try {
            channel.invoker().queryTopicInfo(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds(), promise, builder.build());
            QueryTopicInfoResponse response = promise.get(proxyConfiguration.getProxyLeaderSyncUpstreamTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
            return response.getTopicInfosMap();
        } catch (Throwable t){
            logger.error(t.getMessage(), t);
            return null;
        }
    }


    @Override
    public Map<String, TopicInfo> acquireTopicMetadata(List<String> topics) {
        if (topics.isEmpty()) {
            return acquireFromUpstream(topics, null);
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

    @Override
    public void refreshTopicMetadata(List<String> topics, ClientChannel channel) {
        Map<String, TopicInfo> ret = acquireFromUpstream(topics, channel);
        if (ret == null) {
            for (String topic : topics) {
                invalidTopicMetadata(topic);
            }
            return;
        }

        for (Map.Entry<String, TopicInfo> entry : ret.entrySet()) {
            String topic = entry.getKey();
            TopicInfo info = entry.getValue();
            if(info == null) {
                continue;
            }
            topicMetaLoadingCache.put(topic, info);
        }
    }

    @Override
    public void invalidTopicMetadata(String topic) {
        topicMetaLoadingCache.invalidate(topic);
    }
}
