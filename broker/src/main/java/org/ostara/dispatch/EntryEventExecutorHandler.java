package org.ostara.dispatch;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ostara.ledger.Cursor;
import org.ostara.ledger.Offset;

@SuppressWarnings("all")
public class EntryEventExecutorHandler {

    private final String id = UUID.randomUUID().toString();
    private final ConcurrentMap<Channel, EntrySubscription> channelShips = new ConcurrentHashMap<>();
    private final Object2ObjectMap<String, EntrySubscription> subscribeShips = new Object2ObjectOpenHashMap<>();
    private final EventExecutor dispatchExecutor;
    private volatile Offset nextOffset;
    private volatile Cursor nextCursor;

    private volatile AtomicBoolean triggered = new AtomicBoolean(false);

    public EntryEventExecutorHandler(EventExecutor executor) {
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
