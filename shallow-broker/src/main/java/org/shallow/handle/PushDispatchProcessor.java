package org.shallow.handle;

import io.netty.channel.Channel;
import org.shallow.log.Offset;

public interface PushDispatchProcessor {

    void subscribe(Channel channel, String topic, String queue, Offset offset, short version);

    void clean(Channel channel, String topic, String queue);

    void clearChannel(Channel channel);

    void handle(String topic);

    void shutdownGracefully();
}
