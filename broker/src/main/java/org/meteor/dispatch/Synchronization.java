package org.meteor.dispatch;

import io.netty.channel.Channel;
import org.meteor.common.message.Offset;

public class Synchronization<E> {
    protected final Channel channel;
    protected final E handler;
    protected Offset dispatchOffset;
    protected boolean followed = false;

    public Synchronization(Channel channel, E handler) {
        this.channel = channel;
        this.handler = handler;
    }

    public void setDispatchOffset(Offset dispatchOffset) {
        this.dispatchOffset = dispatchOffset;
    }

    public void setFollowed(boolean followed) {
        this.followed = followed;
    }

    public Channel getChannel() {
        return channel;
    }

    public E getHandler() {
        return handler;
    }

    public Offset getDispatchOffset() {
        return dispatchOffset;
    }

    public boolean isFollowed() {
        return followed;
    }
}
