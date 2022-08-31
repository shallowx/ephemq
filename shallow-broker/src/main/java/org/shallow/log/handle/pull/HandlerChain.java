package org.shallow.log.handle.pull;

public interface HandlerChain {
    Handler applyHandler();

    Handler preHandle(Handler handler);

    void postHandle(Handler handler);
}
