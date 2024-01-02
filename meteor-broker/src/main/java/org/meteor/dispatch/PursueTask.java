package org.meteor.dispatch;

import org.meteor.common.message.Offset;
import org.meteor.ledger.LedgerCursor;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PursueTask<E> {
    private final E subscription;
    private final LedgerCursor cursor;
    private final long pursueTimeMillis = System.currentTimeMillis();
    private Offset pursueOffset;

    public PursueTask(E subscription, LedgerCursor cursor, Offset pursueOffset) {
        this.subscription = subscription;
        this.cursor = cursor;
        this.pursueOffset = pursueOffset;
    }

    public void setPursueOffset(Offset pursueOffset) {
        this.pursueOffset = pursueOffset;
    }

    public E getSubscription() {
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

    @Override
    public String toString() {
        return "pursue_task{" +
                "subscription=" + subscription +
                ", cursor=" + cursor +
                ", pursueTimeMillis=" + pursueTimeMillis +
                ", pursueOffset=" + pursueOffset +
                '}';
    }
}
