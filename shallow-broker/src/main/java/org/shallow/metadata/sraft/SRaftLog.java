package org.shallow.metadata.sraft;

public interface SRaftLog<T> {
    void prepareCommit(T t);
    void postCommit(T t);
}
