package org.shallow.metadata.sraft;

public interface SRaftLog<T> {
    void prepareCommit(T t, CommitType type);
    void postCommit(T t, CommitType type);
}
