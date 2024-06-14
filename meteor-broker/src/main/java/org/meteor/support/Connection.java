package org.meteor.support;

import io.netty.channel.Channel;
import java.util.Set;
import javax.annotation.Nullable;

public interface Connection {
    void add(Channel channel);
    boolean remove(Channel channel);
    @Nullable
    Set<Channel> getReadyChannels();
}
