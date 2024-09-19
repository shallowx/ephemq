package org.meteor.cli.ledger;

/**
 * This class represents a MigrateLedger entity, which is used to specify the details
 * of a ledger migration in a distributed messaging system.
 * The MigrateLedger object contains information about the topic, partition,
 * source broker, and destination broker for the migration.
 */
public class MigrateLedger {
    /**
     * The topic associated with this ledger migration. It specifies the name
     * of the topic that is being migrated within the distributed messaging system.
     */
    private String topic;
    /**
     * The partition number of the topic for which the ledger migration is being performed.
     * This variable indicates the specific partition within a topic that is targeted for migration
     * between brokers.
     */
    private int partition;
    /**
     * The source broker from which the ledger is being migrated.
     */
    private String from;
    /**
     * Represents the destination broker to which the ledger is being migrated.
     */
    private String to;

    /**
     * Constructs a new MigrateLedger object.
     *
     * @param topic     The name of the topic for which the migration is being performed.
     * @param partition The partition number of the topic.
     * @param from      The source broker from which the ledger is being migrated.
     * @param to        The destination broker to which the ledger is being migrated.
     */
    public MigrateLedger(String topic, int partition, String from, String to) {
        this.topic = topic;
        this.partition = partition;
        this.from = from;
        this.to = to;
    }

    /**
     * Retrieves the topic associated with this MigrateLedger instance.
     *
     * @return the topic name.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the topic for the ledger migration.
     *
     * @param topic the topic to be set for the migration
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     *
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Sets the partition number for the ledger migration.
     *
     * @param partition the partition number to be set
     */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /**
     * Gets the source broker from which the ledger is being migrated.
     *
     * @return the source broker identifier
     */
    public String getFrom() {
        return from;
    }

    /**
     * Sets the source broker for the ledger migration.
     *
     * @param from the source broker for the ledger migration.
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * Retrieves the destination broker to which the ledger is being migrated.
     *
     * @return the identifier of the destination broker.
     */
    public String getTo() {
        return to;
    }

    /**
     * Sets the destination broker for the ledger migration.
     *
     * @param to the identifier of the destination broker
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * Returns a string representation of the MigrateLedger object.
     *
     * @return A string containing the topic, partition, from, and to information.
     */
    @Override
    public String toString() {
        return STR."(topic='\{topic}', partition=\{partition}, from='\{from}', to='\{to}')";
    }
}
