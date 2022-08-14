package org.shallow.metadata.sraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Promise;

public interface SRaftLog<T> {
    void prepareCommit(T t, CommitType type, Promise<MessageLite> promise);
    void postCommit(T t, CommitType type);
}
