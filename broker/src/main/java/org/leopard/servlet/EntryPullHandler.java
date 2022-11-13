package org.leopard.servlet;

import io.netty.util.concurrent.EventExecutor;

import java.util.Objects;

public class EntryPullHandler {
    private final int id;
    private final EventExecutor handleExecutor;
    private final int limit;
    private int count;

    public EntryPullHandler(int id, EventExecutor handleExecutor, int limit, int count) {
        this.id = id;
        this.handleExecutor = handleExecutor;
        this.limit = limit;
        this.count = count;
    }

    public int id() {
        return id;
    }

    public EventExecutor executor() {
        return handleExecutor;
    }

    public int limit() {
        return limit;
    }

    public int count() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryPullHandler handler)) return false;
        return id == handler.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
