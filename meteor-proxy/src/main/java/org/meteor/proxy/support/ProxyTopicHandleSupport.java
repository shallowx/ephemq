package org.meteor.proxy.support;

import java.util.List;
import java.util.Map;
import org.meteor.client.core.ClientChannel;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.support.TopicHandleSupport;

public interface ProxyTopicHandleSupport extends TopicHandleSupport {
    Map<String, TopicInfo> getTopicMetadata(List<String> topics);
    void refreshTopicMetadata(List<String> topics, ClientChannel channel);
    void invalidTopicMetadata(String topic);
}
