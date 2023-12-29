package org.meteor.client.internal;

import java.util.Map;

public class MessageRouter {
    private final long token;
    private final String topic;
    private final Map<Integer, MessageLedger> ledgers;
    private final int[] ledgerIds;

    public MessageRouter(long token, String topic, Map<Integer, MessageLedger> ledgers) {
        this.token = token;
        this.topic = topic;
        this.ledgers = ledgers;
        this.ledgerIds = ledgers.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    public long token() {
        return token;
    }

    public String topic() {
        return topic;
    }

    public Map<Integer, MessageLedger> ledgers() {
        return ledgers;
    }

    public MessageLedger ledger(int id) {
        return ledgers.get(id);
    }

    public MessageLedger routeLedger(String queue) {
        int length = ledgerIds.length;
        if (length == 0) {
            return null;
        }

        if (length == 1) {
            return ledgers.get(ledgerIds[0]);
        }

        return ledgers.get(ledgerIds[((31 * topic.hashCode() + queue.hashCode()) & 0x7fffffff) % length]);
    }

    public int routeMarker(String queue) {
        return 31 * queue.hashCode() + topic.hashCode();
    }
}
