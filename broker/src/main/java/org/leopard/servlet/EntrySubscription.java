package org.leopard.servlet;

import io.netty.channel.Channel;
import org.leopard.ledger.Offset;

import java.util.List;

public class EntrySubscription {
    private Channel channel;
    private List<String> queue;
    private Offset offset;
    private short version;
    private String topic;
    private EntryHandler handler;

    private EntrySubscription() {
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

    public String getTopic() {
        return topic;
    }

    public void setOffset(Offset offset) {
        this.offset = offset;
    }

    public EntryHandler getHandler() {
        return handler;
    }

    public static SubscribeBuilder newBuilder() {
        return new SubscribeBuilder();
    }

    public static class SubscribeBuilder {
        private Channel channel;
        private List<String> queue;
        private Offset offset;
        private short version;
        private EntryHandler handler;
        private String topic;

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

        public SubscribeBuilder handler(EntryHandler handler) {
            this.handler = handler;
            return this;
        }

        public SubscribeBuilder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public EntrySubscription build() {
            EntrySubscription subscription = new EntrySubscription();

            subscription.channel = this.channel;
            subscription.queue = this.queue;
            subscription.offset = this.offset;
            subscription.handler = this.handler;
            subscription.version = this.version;
            subscription.topic = this.topic;

            return subscription;
        }
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "channel=" + channel +
                ", queue=" + queue +
                ", offset=" + offset +
                ", version=" + version +
                ", topic='" + topic + '\'' +
                ", handler=" + handler +
                '}';
    }
}
