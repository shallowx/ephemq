package org.ephemq.common.message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The Node class represents a node in a distributed system or cluster.
 * It holds information about the node's identity, network location,
 * registration time, cluster affiliation, state, and auxiliary data.
 */
public class Node {
    /**
     * The unique identifier for the Node.
     */
    private String id;
    /**
     * Represents the hostname or IP address of the node.
     */
    private String host;
    /**
     * The port number on which the Node listens for network connections.
     * Used to identify the specific port instance in the Node's networking setup.
     */
    private int port;
    /**
     * The registration timestamp of the node.
     * Represents the time at which the node was registered in the system.
     * This is stored as a long value representing the number of milliseconds since the epoch (January 1, 1970, 00:00:00 GMT).
     */
    private long registrationTimestamp;
    /**
     * Represents the cluster identifier to which the node belongs.
     * This field helps in logically grouping nodes under a common cluster.
     * Used in clustering algorithms and management tasks.
     */
    private String cluster;
    /**
     * Represents the current state of the Node instance.
     * This state typically indicates the operational status of the Node,
     * such as active, inactive, or any other defined states pertinent to the system.
     */
    private String state;
    /**
     * A map that holds auxiliary data for additional information about the node.
     * The keys are strings that denote the type or category of data,
     * and the values are objects that can store the actual data.
     */
    private Map<String, Object> auxData = new HashMap<>();
    /**
     * A map that stores throughput data for the ledger, where the key represents
     * an integer identifier (e.g., ledger ID) and the value represents the
     * throughput value associated with that identifier.
     */
    private Map<Integer, Integer> ledgerThroughput = new HashMap<>();

    /**
     * Constructs a new Node instance with default values for all fields.
     */
    public Node() {
    }

    /**
     * Constructs a new Node with the provided parameters.
     *
     * @param id                    the unique identifier for the node.
     * @param host                  the host address of the node.
     * @param port                  the port number on which the node is running.
     * @param registrationTimestamp the timestamp when the node was registered.
     * @param cluster               the cluster to which the node belongs.
     * @param state                 the current state of the node.
     */
    public Node(String id, String host, int port, long registrationTimestamp, String cluster, String state) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.registrationTimestamp = registrationTimestamp;
        this.cluster = cluster;
        this.state = state;
    }

    /**
     * Retrieves the identifier of the node.
     *
     * @return the identifier of the node
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the identifier for the node.
     *
     * @param id the unique identifier to be set for the node
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Retrieves the host name of the node.
     *
     * @return the host name as a string
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host for this Node.
     *
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns the port number associated with the node.
     *
     * @return the port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port number for this node.
     *
     * @param port the port number to be set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Retrieves the registration timestamp for this node.
     *
     * @return the registration timestamp as a long value
     */
    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    /**
     * Sets the registration timestamp for the node.
     *
     * @param registrationTimestamp the timestamp indicating when the node was registered.
     */
    public void setRegistrationTimestamp(long registrationTimestamp) {
        this.registrationTimestamp = registrationTimestamp;
    }

    /**
     * Retrieves the cluster name associated with the node.
     *
     * @return the cluster name as a String
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Sets the cluster name for the node.
     *
     * @param cluster the name of the cluster to set
     */
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    /**
     * Retrieves the state of the node.
     *
     * @return the current state of the node as a String
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the state of the node.
     *
     * @param state the new state to set for the node
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * Retrieves the auxiliary data associated with this node.
     *
     * @return a map containing auxiliary data where keys are strings and values are objects
     */
    public Map<String, Object> getAuxData() {
        return auxData;
    }

    /**
     * Sets the auxiliary data for the node.
     *
     * @param auxData a map containing auxiliary data where the key is a string identifier and the value is an object
     */
    public void setAuxData(Map<String, Object> auxData) {
        this.auxData = auxData;
    }

    /**
     * Retrieves the current ledger throughput statistics.
     *
     * @return a map where the key is an integer representing a ledger ID and the value is an integer representing the throughput of the corresponding ledger.
     */
    public Map<Integer, Integer> getLedgerThroughput() {
        return ledgerThroughput;
    }

    /**
     *
     */
    public void setLedgerThroughput(Map<Integer, Integer> ledgerThroughput) {
        this.ledgerThroughput = ledgerThroughput;
    }

    /**
     * Determines whether the specified object is equal to this Node.
     *
     * @param o the reference object with which to compare
     * @return {@code true} if this Node is the same as the object argument; {@code false} otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(id, node.id) && Objects.equals(cluster, node.cluster);
    }

    /**
     * Generates a hash code for the Node object based on its fields: id, host, port,
     * registrationTimestamp, cluster, state, auxData, and ledgerThroughput.
     *
     * @return a hash code value for this Node object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, host, port, registrationTimestamp, cluster, state, auxData, ledgerThroughput);
    }

    /**
     * Returns a string representation of this Node object.
     *
     * @return a string representation of the object, including the id, host, port,
     *         registrationTimestamp, cluster, state, auxData, and ledgerThroughput.
     */
    @Override
    public String toString() {
        return "Node (id='%s', host='%s', port=%d, registrationTimestamp=%d, cluster='%s', state='%s', auxData=%s, ledgerThroughput=%s)".formatted(id, host, port, registrationTimestamp, cluster, state, auxData, ledgerThroughput);
    }
}
