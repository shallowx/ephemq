package org.shallow.consumer.pull;

import io.netty.util.concurrent.Promise;
import org.shallow.consumer.MessagePostFilter;
import org.shallow.proto.server.PullMessageResponse;

public interface PullConsumer {

    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    void registerListener(MessagePullListener listener);

    void registerFilter(MessagePostFilter filter);


    void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, int epoch, long index, int limit) throws Exception;

    void pull(String topic, String queue, short version, int epoch, long index, int limit, MessagePullListener listener,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, short version, int epoch, long index, int limit, MessagePullListener listener) throws Exception;

    void pull(String topic, String queue, short version, int epoch, long index, int limit,  Promise<PullMessageResponse> promise) throws Exception;

    void pull(String topic, String queue, short version, int epoch, long index, int limit) throws Exception;
    MessagePullListener getListener();
}
