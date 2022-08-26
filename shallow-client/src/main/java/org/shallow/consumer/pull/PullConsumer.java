package org.shallow.consumer.pull;

import org.shallow.consumer.pull.MessagePullListener;

public interface PullConsumer {
    void pull(String topic, String queue, int limit) throws Exception;
    MessagePullListener getPullListener();
}
