package org.shallow.consumer.pull;

interface PullConsumer {
    void pull(String topic, String queue, int limit) throws Exception;
    MessagePullListener getPullListener();
}
