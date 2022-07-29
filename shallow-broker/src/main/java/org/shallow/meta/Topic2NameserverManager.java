package org.shallow.meta;

import io.netty.util.concurrent.Promise;
import org.shallow.ClientConfig;
import org.shallow.ShutdownHook;
import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.server.CreateTopicResponse;
import org.shallow.proto.server.DelTopicResponse;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.NameServer.NEW_TOPIC;
import static org.shallow.processor.ProcessCommand.NameServer.REMOVE_TOPIC;

public class Topic2NameserverManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ShutdownHook.class);

    private final ClientConfig config;
    private final BrokerManager manager;

    public Topic2NameserverManager(ClientConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;
    }

    public Map<String, TopicInfo> getTopicMetadata(String topic) {
        return null;
    }

    public void write2Nameserver(String topic, int partitions, int latency, Promise<CreateTopicResponse> promise) {
        final TopicManager topicManager = manager.getTopicManager();
        try {
            Promise<CreateTopicResponse> responsePromise = topicManager.createTopic(NEW_TOPIC, topic, partitions, latency);
            promise.trySuccess(responsePromise.get(config.getConnectTimeOutMs(), TimeUnit.MILLISECONDS));
        } catch (Throwable t) {
            promise.tryFailure(new RuntimeException(String.format("[write2Nameserver] - failed to create topic: topic<%s> partitions<%s> latency<%s>", topic, partitions, latency), t));
        }
    }

    public void delFormNameserver(String topic, Promise<DelTopicResponse> promise) {
        final TopicManager topicManager = manager.getTopicManager();
        try {
            Promise<DelTopicResponse> responsePromise = topicManager.delTopic(REMOVE_TOPIC, topic);
            promise.trySuccess(responsePromise.get(config.getConnectTimeOutMs(), TimeUnit.MILLISECONDS));
        } catch (Throwable t) {
            promise.tryFailure(new RuntimeException(String.format("[write2Nameserver] - failed to create topic: topic<%s>", topic), t));
        }
    }
}
