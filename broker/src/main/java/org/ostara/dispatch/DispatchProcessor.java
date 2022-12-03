package org.ostara.dispatch;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import org.ostara.common.metadata.Subscription;
import org.ostara.ledger.Offset;

public interface DispatchProcessor {

    void subscribe(Channel channel, String topic, String queue, Offset offset, short version,
                   Promise<Subscription> subscribePromise);

    void clean(Channel channel, String topic, String queue, Promise<Void> promise);

    void clearChannel(Channel channel);

    void handleRequest(String topic);

    void shutdownGracefully();
}
