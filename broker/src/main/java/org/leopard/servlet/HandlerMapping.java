package org.leopard.servlet;

public interface HandlerMapping {
    EntryPullHandler applyHandler();

    EntryPullHandler preHandle(EntryPullHandler handler);

    void postHandle(EntryPullHandler handler);

    void close();
}

