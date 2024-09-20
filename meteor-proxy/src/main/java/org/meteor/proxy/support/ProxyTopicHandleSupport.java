package org.meteor.proxy.support;

import java.util.List;
import java.util.Map;
import org.meteor.client.core.ClientChannel;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.support.TopicHandleSupport;

/**
 * Interface ProxyTopicHandleSupport extends TopicHandleSupport with additional
 * methods focused on managing and refreshing topic metadata.
 */
public interface ProxyTopicHandleSupport extends TopicHandleSupport {
    /**
     * Retrieves metadata information for the specified list of topics.
     *
     * @param topics A list of topic names for which metadata is requested.
     * @return A map containing topic names as keys and their corresponding metadata as values.
     */
    Map<String, TopicInfo> getTopicMetadata(List<String> topics);

    /**
     * Refreshes the metadata of the specified topics using the provided client channel.
     *
     * @param topics  the list of topic names whose metadata needs to be refreshed
     * @param channel the client channel used to perform the metadata refresh operation
     */
    void refreshTopicMetadata(List<String> topics, ClientChannel channel);

    /**
     * Invalidates the metadata for the specified topic, causing the system to disregard the current
     * metadata and potentially request fresh metadata from the upstream source.
     *
     * @param topic the name of the topic for which the metadata should be invalidated
     */
    void invalidTopicMetadata(String topic);
}
