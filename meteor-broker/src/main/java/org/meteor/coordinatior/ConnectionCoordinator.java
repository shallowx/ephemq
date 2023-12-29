package org.meteor.coordinatior;

import io.netty.channel.Channel;

import java.util.Set;

public interface ConnectionCoordinator {
    void add(Channel channel);

    boolean remove(Channel channel);

    Set<Channel> getChannels();
}
