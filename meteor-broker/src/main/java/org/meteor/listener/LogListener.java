package org.meteor.listener;

import org.meteor.ledger.Log;

/**
 * A listener interface for monitoring log-related events.
 */
public interface LogListener {
    /**
     * Called when a log is initialized.
     *
     * @param log the log that has been initialized
     */
    void onInitLog(Log log);

    /**
     * Called when a message is received.
     *
     * @param topic  the topic to which the message belongs
     * @param ledger the ledger ID associated with the message
     * @param count  the number of messages received
     */
    void onReceiveMessage(String topic, int ledger, int count);

    /**
     * Called when there is a synchronization message for a specific topic and ledger.
     *
     * @param topic the topic associated with the synchronization message
     * @param ledger the identifier of the ledger for which the message is relevant
     * @param count the number of records to be synchronized
     */
    void onSyncMessage(String topic, int ledger, int count);

    /**
     * Handles the arrival of a push message related to a specific topic and ledger.
     *
     * @param topic  the topic associated with the push message
     * @param ledger the ledger number associated with the topic
     * @param count  the number of messages being pushed
     */
    void onPushMessage(String topic, int ledger, int count);

    /**
     * Handles messages when a chunk of log entries is pushed.
     *
     * @param topic The topic of the log entries.
     * @param ledger The ledger ID where the log entries are stored.
     * @param count The number of log entries in the chunk.
     */
    void onChunkPushMessage(String topic, int ledger, int count);
}
