package org.meteor.expand.api;

public interface ConsumeQueue {
    long index();

    boolean updateIndex(long index);
}
