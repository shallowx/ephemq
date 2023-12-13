package org.meteor.proxy.coordinatio;

import org.meteor.client.internal.ClientChannel;
import org.meteor.coordinatio.TopicCoordinator;
import org.meteor.remote.proto.TopicInfo;

import java.util.List;
import java.util.Map;

public interface ProxyTopicCoordinator extends TopicCoordinator {
    Map<String, TopicInfo> acquireTopicMetadata(List<String> topics);
    void refreshTopicMetadata(List<String> topics, ClientChannel channel);
    void invalidTopicMetadata(String topic);
}
