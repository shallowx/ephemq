package org.shallow.nraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Promise;

public interface LeaderElector {

    void start()  throws Exception;

    void respond(int term, Promise<? super MessageLite> promise);

    int getTerm();

    void keepHeartbeat();

    boolean isLeader();

    boolean isFollower();

    boolean isCandidate();

    boolean hasLeader();

}
