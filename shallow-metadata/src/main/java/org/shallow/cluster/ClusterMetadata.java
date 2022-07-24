package org.shallow.cluster;

import java.util.List;

public class ClusterMetadata {
    private final String id;
    private List<Node> nodes;

    public ClusterMetadata(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }
}
