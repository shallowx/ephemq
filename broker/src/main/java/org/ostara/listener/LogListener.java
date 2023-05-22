package org.ostara.listener;

import org.ostara.log.Log;

public interface LogListener {
    void onInitLog(Log log);
    void onReceiveMessage(String topic, int ledger, int count);
    void onSyncMessage(String topic, int ledger, int count);
    void onPushMessage(String topic, int ledger, int count);
    void onChunkPushMessage(String topic, int ledger, int count);
}
