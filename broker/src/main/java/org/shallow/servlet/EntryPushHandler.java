package org.shallow.servlet;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.objects.*;
import org.shallow.ledger.Cursor;
import org.shallow.ledger.Offset;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("all")
public class EntryPushHandler {

    private final String id = UUID.randomUUID().toString();
    private final ConcurrentMap<Channel, EntrySubscription> channelShips = new ConcurrentHashMap<>();
    private final Object2ObjectMap<String, EntrySubscription> subscribeShips = new Object2ObjectOpenHashMap<>();
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

    public ConcurrentMap<Channel, EntrySubscription> getChannelShips() {
        return channelShips;
    }

    public Object2ObjectMap<String, EntrySubscription> getSubscribeShips() {
        return subscribeShips;
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
                ", channelShips=" + channelShips +
                ", dispatchExecutor=" + dispatchExecutor +
                ", nextOffset=" + nextOffset +
                ", nextCursor=" + nextCursor +
                ", triggered=" + triggered +
                '}';
    }
}
