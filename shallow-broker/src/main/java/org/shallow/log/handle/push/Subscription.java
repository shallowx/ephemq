package org.shallow.log.handle.push;

import io.netty.channel.Channel;
import org.shallow.log.Offset;

import java.util.List;

public class Subscription {
    private Channel channel;
    private List<String> queue;
    private Offset offset;
    private short version;
    private EntryPushHandler handler;

    private Subscription() {
        //unsupported
    }

    public List<String> getQueue() {
        return queue;
    }

    public Channel getChannel() {
        return channel;
    }

    public Offset getOffset() {
        return offset;
    }

    public short getVersion() {
        return version;
    }

    public void setOffset(Offset offset) {
        this.offset = offset;
    }

    public static SubscribeBuilder newBuilder() {
        return new SubscribeBuilder();
    }

    public static class SubscribeBuilder {
        private Channel channel;
        private List<String> queue;
        private Offset offset;
        private short version;
        private EntryPushHandler handler;

        private SubscribeBuilder() {
        }

        public SubscribeBuilder channel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public SubscribeBuilder queue(List<String> queue) {
            this.queue = queue;
            return this;
        }

        public SubscribeBuilder offset(Offset offset) {
            this.offset = offset;
            return this;
        }

        public SubscribeBuilder version(short version) {
            this.version = version;
            return this;
        }
        public SubscribeBuilder handler(EntryPushHandler handler) {
            this.handler = handler;
            return this;
        }

        public Subscription build() {
            Subscription subscription = new Subscription();

            subscription.channel = this.channel;
            subscription.queue = this.queue;
            subscription.offset = this.offset;
            subscription.handler = this.handler;
            subscription.version = this.version;

            return subscription;
        }
    }
}
