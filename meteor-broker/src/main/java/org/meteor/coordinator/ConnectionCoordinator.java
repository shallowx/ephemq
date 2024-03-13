package org.meteor.coordinator;

import io.netty.channel.Channel;

import javax.annotation.Nullable;
import java.util.Set;

public interface ConnectionCoordinator {
    void add(Channel channel);

    boolean remove(Channel channel);

    @Nullable
    Set<Channel> getReadyChannels();
}
