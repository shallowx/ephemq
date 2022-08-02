package org.shallow.meta;

import java.util.Objects;

public class Topic {
    private String name;
    private Partition partitionInfo;

    public Topic(String name, Partition partitionInfo) {
        this.name = name;
        this.partitionInfo = partitionInfo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Partition getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(Partition partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic topic)) return false;
        return getName().equals(topic.getName()) &&
                getPartitionInfo().equals(topic.getPartitionInfo());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitionInfo());
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name='" + name + '\'' +
                ", partitionInfo=" + partitionInfo +
                '}';
    }
}
