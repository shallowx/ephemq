package org.ostara.management;

import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultConnectionManager implements ConnectionManager {

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
