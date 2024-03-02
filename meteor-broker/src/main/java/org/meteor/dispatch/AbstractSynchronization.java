package org.meteor.dispatch;

import io.netty.channel.Channel;
import org.meteor.common.message.Offset;

abstract class AbstractSynchronization<T> {
    protected final Channel channel;
    protected final T handler;
    protected Offset dispatchOffset;
    protected volatile boolean followed = false;

    public AbstractSynchronization(Channel channel, T handler) {
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

    public T getHandler() {
        return handler;
    }

    public Offset getDispatchOffset() {
        return dispatchOffset;
    }

    public boolean isFollowed() {
        return followed;
    }
}
