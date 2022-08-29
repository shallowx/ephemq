package org.shallow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.shallow.util.ObjectUtil.isNull;

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
                ", extras=" + extras +
                '}';
    }

    public static class Extras implements Iterable<Map.Entry<String, String>> {

        private final Map<String, String> extras;

        public Extras() {
            this(null);
        }

        public Extras(Map<String, String> extras) {
            this.extras = extras == null ? new HashMap<>() : extras;
        }

        public String getValue(String key) {
            return extras.get(key);
        }

        public String getKey(String value) {
            return extras.entrySet().stream()
                    .filter(entry -> {
                        String v = entry.getValue();
                        return v.equals(value);
                    }).map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
        }

        public boolean contains(String key) {
            return extras.containsKey(key);
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return extras.entrySet().iterator();
        }

        @Override
        public String toString() {
            return "Extras{" +
                    "extras=" + extras +
                    '}';
        }
    }
}
