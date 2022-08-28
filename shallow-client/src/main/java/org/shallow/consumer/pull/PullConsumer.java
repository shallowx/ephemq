package org.shallow.consumer.pull;

import io.netty.util.concurrent.Promise;
import org.shallow.proto.server.PullMessageResponse;

public interface PullConsumer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    void registerMessageListener(MessagePullListener listener);
    void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit) throws Exception;
    MessagePullListener getPullListener();
}
