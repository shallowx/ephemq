package org.meteor.support;

import io.netty.channel.Channel;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultConnectionCoordinator implements ConnectionCoordinator {
    private final Set<Channel> readyChannels = new CopyOnWriteArraySet<>();

    @Override
    public void add(Channel channel) {
        if (!channel.isActive()) {
            return;
        }
        readyChannels.add(channel);
    }

    @Override
    public boolean remove(Channel channel) {
        return readyChannels.remove(channel);
    }

    @Override
    public Set<Channel> getReadyChannels() {
        return readyChannels;
    }
}
