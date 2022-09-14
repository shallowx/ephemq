package org.shallow.handle;

public interface HandlerMapping {
    EntryPullHandler applyHandler();

    EntryPullHandler preHandle(EntryPullHandler handler);

    void postHandle(EntryPullHandler handler);
}
