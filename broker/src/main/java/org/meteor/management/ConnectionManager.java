package org.meteor.management;

import io.netty.channel.Channel;

import java.util.Set;

public interface ConnectionManager {
    void add(Channel channel);

    boolean remove(Channel channel);

    Set<Channel> getChannels();
}
