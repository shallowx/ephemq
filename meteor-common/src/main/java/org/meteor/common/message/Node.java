package org.meteor.common.message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Node {
    private String id;
    private String host;
    private int port;
    private long registrationTimestamp;
    private String cluster;
    private String state;
    private Map<String, Object> auxData = new HashMap<>();
    private Map<Integer, Integer> ledgerThroughput = new HashMap<>();

    public Node() {
    }

    public Node(String id, String host, int port, long registrationTimestamp, String cluster, String state) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.registrationTimestamp = registrationTimestamp;
        this.cluster = cluster;
        this.state = state;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getRegistrationTimestamp() {
        return registrationTimestamp;
    }

    public void setRegistrationTimestamp(long registrationTimestamp) {
        this.registrationTimestamp = registrationTimestamp;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, Object> getAuxData() {
        return auxData;
    }

    public void setAuxData(Map<String, Object> auxData) {
        this.auxData = auxData;
    }

    public Map<Integer, Integer> getLedgerThroughput() {
        return ledgerThroughput;
    }

    public void setLedgerThroughput(Map<Integer, Integer> ledgerThroughput) {
        this.ledgerThroughput = ledgerThroughput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(id, node.id) && Objects.equals(cluster, node.cluster);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port, registrationTimestamp, cluster, state, auxData, ledgerThroughput);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", registrationTimestamp=" + registrationTimestamp +
                ", cluster='" + cluster + '\'' +
                ", state='" + state + '\'' +
                ", auxData=" + auxData +
                ", ledgerThroughput=" + ledgerThroughput +
                '}';
    }
}
