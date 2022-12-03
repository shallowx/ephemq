package org.ostara.internal.metrics;

import org.ostara.ledger.Ledger;

public interface LedgerMetricsListener {
    void onInitLedger(Ledger ledger);

    void onReceiveMessage(String topic, String queue, int ledger, int count);

    void onDispatchMessage(String topic, int ledger, int count);
}
