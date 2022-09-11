package org.shallow.log.handle.push;

import io.netty.channel.Channel;
import org.shallow.log.Offset;

public interface PushDispatcher {

    void subscribe(Channel channel, String queue, Offset offset, short version);

    void clean(Channel channel, String queue);

    void handle(String topic);

    void shutdownGracefully();
}
