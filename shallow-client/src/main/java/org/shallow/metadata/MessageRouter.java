package org.shallow.metadata;

import java.util.Arrays;
import java.util.Map;

public class MessageRouter {
    private final String topic;
    private final int[] ledgerIds;
    private final Map<Integer, MessageRoutingHolder> holders;

    public MessageRouter(String topic, Map<Integer, MessageRoutingHolder> holders) {
        this.topic = topic;
        this.holders = holders;
        this.ledgerIds = holders.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    public MessageRoutingHolder allocRouteHolder(String queue) {
        int length = ledgerIds.length;

        if (length == 0) {
            return null;
        } else if (length == 1) {
            return holders.get(0);
        }

        int index = ((31 * topic.hashCode() + queue.hashCode()) & 0x7fffffff) % length;
        return holders.get(ledgerIds[index]);
    }

    public Map<Integer, MessageRoutingHolder> getHolders() {
        return holders;
    }

    @Override
    public String toString() {
        return "MessageRouter{" +
                "topic='" + topic + '\'' +
                ", ledgerIds=" + Arrays.toString(ledgerIds) +
                ", holders=" + holders +
                '}';
    }
}
