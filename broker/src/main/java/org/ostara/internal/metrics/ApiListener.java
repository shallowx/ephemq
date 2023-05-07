package org.ostara.internal.metrics;

public interface ApiListener {
   void onCommand(int code, int bytes, long cost, boolean ret);
}
