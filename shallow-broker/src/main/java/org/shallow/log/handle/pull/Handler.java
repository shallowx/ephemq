package org.shallow.log.handle.pull;

import io.netty.util.concurrent.EventExecutor;

import java.util.Objects;

public class Handler {
    private final int id;
    private final EventExecutor handleExecutor;
    private final int limit;
    private int count;

    public Handler(int id, EventExecutor handleExecutor, int limit, int count) {
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
        if (!(o instanceof Handler handler)) return false;
        return id == handler.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
