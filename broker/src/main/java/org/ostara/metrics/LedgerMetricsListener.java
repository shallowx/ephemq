package org.ostara.metrics;

import org.ostara.common.Node;
import org.ostara.ledger.Ledger;

public interface LedgerMetricsListener {
    void onInitLedger(Ledger ledger);

    void onReceiveMessage(String topic, String queue, int ledger, int count);

    void onDispatchMessage(String topic, int ledger, int count);

    void onPartitionInit();
    void onPartitionDestroy();
    void startUp(Node node);
}
