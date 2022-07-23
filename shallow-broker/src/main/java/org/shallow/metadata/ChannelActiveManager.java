package org.shallow.metadata;

import io.netty.channel.Channel;

public interface ChannelActiveManager {
    boolean add(Channel channel);
    boolean remove(Channel channel);
    boolean obtain();
}
