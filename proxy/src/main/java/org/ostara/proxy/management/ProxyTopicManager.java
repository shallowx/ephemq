package org.ostara.proxy.management;

import org.ostara.client.internal.ClientChannel;
import org.ostara.management.TopicManager;
import org.ostara.remote.proto.TopicInfo;

import java.util.List;
import java.util.Map;

public interface ProxyTopicManager extends TopicManager {
    Map<String, TopicInfo> acquireTopicMetadata(List<String> topics);
    void refreshTopicMetadata(List<String> topics, ClientChannel channel);
    void invalidTopicMetadata(String topic);
}
