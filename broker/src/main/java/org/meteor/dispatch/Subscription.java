package org.meteor.dispatch;

import io.netty.channel.Channel;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.meteor.common.message.Offset;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Subscription {
    private final Channel channel;
    private final IntSet markers;
    private final Handler handler;
    private Offset dispatchOffset;
    private boolean followed = false;

    public Subscription(Channel channel, IntSet markers, Handler handler) {
        this.channel = channel;
        this.markers = markers;
        this.handler = handler;
    }

    public void setDispatchOffset(Offset dispatchOffset) {
        this.dispatchOffset = dispatchOffset;
    }

    public IntSet getMarkers() {
        return markers;
    }

    public Channel getChannel() {
        return channel;
    }

    public Handler getHandler() {
        return handler;
    }

    public Offset getDispatchOffset() {
        return dispatchOffset;
    }

    public void setFollowed(boolean followed) {
        this.followed = followed;
    }

    public boolean isFollowed() {
        return followed;
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "channel=" + channel +
                ", markers=" + markers +
                ", handler=" + handler +
                ", dispatchOffset=" + dispatchOffset +
                ", followed=" + followed +
                '}';
    }
}
