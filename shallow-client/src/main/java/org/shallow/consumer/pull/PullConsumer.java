package org.shallow.consumer.pull;

import io.netty.util.concurrent.Promise;
import org.shallow.proto.server.PullMessageResponse;

interface PullConsumer {
    void pull(String topic, String queue, int epoch, long index, int limit, Promise<PullMessageResponse> promise) throws Exception;
    MessagePullListener getPullListener();
}
