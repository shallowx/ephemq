package org.meteor.dispatch;

import javax.annotation.concurrent.Immutable;
import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

@Immutable
final class PursueTask<T> {
    private final T subscription;
    private final LedgerCursor cursor;
    private final long pursueTimeMillis = System.currentTimeMillis();
    private Offset pursueOffset;

    public PursueTask(T subscription, LedgerCursor cursor, Offset pursueOffset) {
        this.subscription = subscription;
        this.cursor = cursor;
        this.pursueOffset = pursueOffset;
    }

    public T getSubscription() {
        return subscription;
    }

    public LedgerCursor getCursor() {
        return cursor;
    }

    public long getPursueTimeMillis() {
        return pursueTimeMillis;
    }

    public Offset getPursueOffset() {
        return pursueOffset;
    }

    public void setPursueOffset(Offset pursueOffset) {
        this.pursueOffset = pursueOffset;
    }

    @Override
    public String toString() {
        return "(" +
                "subscription=" + subscription +
                ", cursor=" + cursor +
                ", pursueTimeMillis=" + pursueTimeMillis +
                ", pursueOffset=" + pursueOffset +
                ')';
    }
}
