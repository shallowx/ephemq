package org.ostara.metrics;

public interface ApiMetricsListener {
   void onCommand(int code, int bytes, long cost, boolean ret);
}
