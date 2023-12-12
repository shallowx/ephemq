package org.meteor.listener;

public interface APIListener {
    void onCommand(int code, int bytes, long cost, boolean result);
}
