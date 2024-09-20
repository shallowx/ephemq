package org.meteor.client.core;

import java.util.Map;

/**
 * The MessageRouter class is responsible for routing messages to the appropriate
 * MessageLedger based on the provided topic and queue.
 */
public class MessageRouter {
    /**
     * A unique token assigned to the message router instance.
     * This token is used to identify individual message routing instances.
     */
    private final long token;
    /**
     * The topic associated with the MessageRouter. It determines which messages
     * the router will handle and route to the appropriate MessageLedger.
     */
    private final String topic;
    /**
     * A map of ledger identifiers to their corresponding MessageLedger instances.
     * Each entry in the map associates an integer ID with a MessageLedger object,
     * which represents a specific partition of a topic in a distributed messaging system.
     */
    private final Map<Integer, MessageLedger> ledgers;
    /**
     * An array of ledger IDs representing the unique identifiers
     * for each MessageLedger managed by this MessageRouter instance.
     */
    private final int[] ledgerIds;

    /**
     * Constructs a MessageRouter with the specified token, topic, and ledgers.
     *
     * @param token   a unique identifier for the router.
     * @param topic   the topic to which this router is assigned.
     * @param ledgers a map of integer keys to MessageLedger objects, representing the available ledgers for this router.
     */
    public MessageRouter(long token, String topic, Map<Integer, MessageLedger> ledgers) {
        this.token = token;
        this.topic = topic;
        this.ledgers = ledgers;
        this.ledgerIds = ledgers.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
    }

    /**
     * Returns the token associated with this MessageRouter instance.
     *
     * @return the token value
     */
    public long token() {
        return token;
    }

    /**
     * Retrieves the topic associated with this MessageRouter.
     *
     * @return the topic string that this MessageRouter routes messages for
     */
    public String topic() {
        return topic;
    }

    /**
     * Retrieves the map of MessageLedgers associated with this MessageRouter.
     *
     * @return a map where the key is the ledger ID and the value is the corresponding MessageLedger.
     */
    public Map<Integer, MessageLedger> ledgers() {
        return ledgers;
    }

    /**
     * Retrieves the MessageLedger associated with the specified identifier.
     *
     * @param id the unique identifier of the MessageLedger to retrieve
     * @return the MessageLedger associated with the given id, or null if no ledger is found for the id
     */
    public MessageLedger ledger(int id) {
        return ledgers.get(id);
    }

    /**
     * Routes the given queue to an appropriate MessageLedger based on internal routing logic.
     *
     * @param queue the name of the queue to be routed
     * @return the MessageLedger associated with the provided queue, or null if no ledgers are available
     */
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

    /**
     * Calculates a hash-based route marker for the given queue in the context of the current topic.
     *
     * @param queue the name of the queue for which the route marker is to be calculated
     * @return an integer representing the hash-based route marker for the given queue
     */
    public int routeMarker(String queue) {
        return 31 * queue.hashCode() + topic.hashCode();
    }
}
