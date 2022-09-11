package org.shallow.log.handle.push;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.log.Cursor;
import org.shallow.log.Offset;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class EntryPushHandler {

    private final String id = UUID.randomUUID().toString();
    private final ConcurrentMap<Channel, Subscription> subscriptionShips = new ConcurrentHashMap<>();
    private final EventExecutor dispatchExecutor;
    private volatile Offset nextOffset;
    private volatile Cursor nextCursor;

    private volatile AtomicBoolean triggered = new AtomicBoolean(false);

    public EntryPushHandler(EventExecutor executor) {
        this.dispatchExecutor = executor;
    }

    public String getId() {
        return id;
    }

    public ConcurrentMap<Channel, Subscription> getSubscriptionShips() {
        return subscriptionShips;
    }

    public EventExecutor getDispatchExecutor() {
        return dispatchExecutor;
    }

    public Offset getNextOffset() {
        return nextOffset;
    }

    public Cursor getNextCursor() {
        return nextCursor;
    }

    public AtomicBoolean getTriggered() {
        return triggered;
    }


    public void setNextOffset(Offset nextOffset) {
        this.nextOffset = nextOffset;
    }

    public void setNextCursor(Cursor nextCursor) {
        this.nextCursor = nextCursor;
    }

    @Override
    public String toString() {
        return "EntryPushHandler{" +
                "id='" + id + '\'' +
                ", subscriptionShips=" + subscriptionShips +
                ", dispatchExecutor=" + dispatchExecutor +
                ", nextOffset=" + nextOffset +
                ", nextCursor=" + nextCursor +
                ", triggered=" + triggered +
                '}';
    }
}
