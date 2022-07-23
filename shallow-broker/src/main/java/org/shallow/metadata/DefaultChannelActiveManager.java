package org.shallow.metadata;

import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class DefaultChannelActiveManager implements ChannelActiveManager{

    private final Set<Channel> channels;

    public DefaultChannelActiveManager() {
        this.channels = new ConcurrentSkipListSet<>();
    }

    @Override
    public boolean add(Channel channel) {
        return false;
    }

    @Override
    public boolean remove(Channel channel) {
        return false;
    }

    @Override
    public boolean obtain() {
        return false;
    }
}
