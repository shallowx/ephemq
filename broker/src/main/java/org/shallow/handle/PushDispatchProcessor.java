package org.shallow.handle;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import org.shallow.client.consumer.push.Subscription;
import org.shallow.ledger.Offset;

public interface PushDispatchProcessor {

    void subscribe(Channel channel, String topic, String queue, Offset offset, short version, Promise<Subscription> subscribePromise);

    void clean(Channel channel, String topic, String queue, Promise<Void> promise);

    void clearChannel(Channel channel);

    void handle(String topic);

    void shutdownGracefully();
}
