package org.meteor.proxy.management;

import org.meteor.client.internal.ClientChannel;
import org.meteor.management.TopicManager;
import org.meteor.remote.proto.TopicInfo;

import java.util.List;
import java.util.Map;

public interface ProxyTopicManager extends TopicManager {
    Map<String, TopicInfo> acquireTopicMetadata(List<String> topics);
    void refreshTopicMetadata(List<String> topics, ClientChannel channel);
    void invalidTopicMetadata(String topic);
}
