package org.shallow.meta;

public class ClusterInfo {
    private String name;
    private Node node;

    public ClusterInfo(String name, Node node) {
        this.name = name;
        this.node = node;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
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
