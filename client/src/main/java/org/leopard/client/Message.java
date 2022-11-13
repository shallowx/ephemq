package org.leopard.client;

import com.google.common.base.Joiner;

import java.util.Arrays;

public class Message {

    private final String topic;
    private final String queue;
    private final byte[] message;
    private int epoch;
    private long index;
    private final short version;
    private final Extras extras;

    public Message(String topic, String queue, byte[] message, Extras extras) {
       this(topic, queue, (short) -1, message, extras);
    }

    public Message(String topic, String queue, byte[] message, int epoch, long index, Extras extras) {
        this(topic, queue, (short) -1, message, epoch, index, extras);
    }

    public Message(String topic, String queue, short version, byte[] message, Extras extras) {
        this.topic = topic;
        this.queue = queue;
        this.version = version;
        this.message = message;
        this.extras = extras ;
    }

    public Message(String topic, String queue, short version, byte[] message, int epoch, long index, Extras extras) {
        this.topic = topic;
        this.queue = queue;
        this.version = version;
        this.message = message;
        this.epoch = epoch;
        this.index = index;
        this.extras = extras;
    }


    public short version() {
        return version;
    }

    public String topic() {
        return topic;
    }

    public String queue() {
        return queue;
    }

    public byte[] message() {
        return message;
    }

    public int epoch() {
        return epoch;
    }

    public long index() {
        return index;
    }

    public Extras extras() {
        return extras;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", queue='" + queue + '\'' +
                ", version=" + version +
                ", message=" + Arrays.toString(message) +
                ", epoch=" + epoch +
                ", index=" + index +
                ", extras=" + Joiner.on(",").withKeyValueSeparator("=").join(extras) +
                '}';
    }
}
