package org.meteor.proxy.coordinator;

import java.util.List;
import java.util.Map;
import org.meteor.client.ClientChannel;
import org.meteor.coordinator.TopicCoordinator;
import org.meteor.remote.proto.TopicInfo;

public interface ProxyTopicCoordinator extends TopicCoordinator {
    Map<String, TopicInfo> getTopicMetadata(List<String> topics);

    void refreshTopicMetadata(List<String> topics, ClientChannel channel);

    void invalidTopicMetadata(String topic);
}
