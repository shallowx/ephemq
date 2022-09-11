package org.shallow.log.handle.pull;

public interface HandlerChain {
    EntryPullHandler applyHandler();

    EntryPullHandler preHandle(EntryPullHandler handler);

    void postHandle(EntryPullHandler handler);
}
