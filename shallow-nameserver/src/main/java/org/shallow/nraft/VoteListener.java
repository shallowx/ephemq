package org.shallow.nraft;

import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.Promise;

public interface VoteListener {
    void vote(Promise<Boolean> promise);

    void answer(int term, Promise<? super MessageLite> promise);

    void revert();

}
