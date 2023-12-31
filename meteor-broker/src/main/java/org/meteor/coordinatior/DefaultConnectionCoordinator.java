package org.meteor.coordinatior;

import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultConnectionCoordinator implements ConnectionCoordinator {
    private final Set<Channel> channels = new CopyOnWriteArraySet<>();
    @Override
    public void add(Channel channel) {
        channels.add(channel);
    }
    @Override
    public boolean remove(Channel channel) {
        return channels.remove(channel);
    }
    @Override
    public Set<Channel> getChannels() {
        return channels;
    }
}
