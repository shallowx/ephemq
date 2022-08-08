package org.shallow.meta;

public class ClusterRecord {
    private String name;
    private NodeRecord node;

    public ClusterRecord(String name, NodeRecord node) {
        this.name = name;
        this.node = node;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public NodeRecord getNode() {
        return node;
    }

    public void setNode(NodeRecord node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "name='" + name + '\'' +
                ", node=" + node +
                '}';
    }
}
