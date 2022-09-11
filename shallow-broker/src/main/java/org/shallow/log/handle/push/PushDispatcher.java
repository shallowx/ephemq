package org.shallow.log.handle.push;

import io.netty.channel.Channel;
import org.shallow.log.Offset;

public interface PushDispatcher {

    void subscribe(Channel channel, String topic, String queue, Offset offset, short version);

    void clean(Channel channel, String topic, String queue);

    void clearChannel(Channel channel);

    void handle(String topic);

    void shutdownGracefully();
}
