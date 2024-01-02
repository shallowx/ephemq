package org.meteor.coordinatior;

import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultConnectionCoordinator implements ConnectionCoordinator {
    private final Set<Channel> activeChannels = new CopyOnWriteArraySet<>();
    @Override
    public void add(Channel channel) {
        if (!channel.isActive()) {
            return;
        }
        activeChannels.add(channel);
    }
    @Override
    public boolean remove(Channel channel) {
        return activeChannels.remove(channel);
    }
    @Override
    public Set<Channel> getActiveChannels() {
        return activeChannels;
    }
}
