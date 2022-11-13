package org.leopard.internal.metrics;

import org.leopard.ledger.Ledger;

public interface LedgerMetricsListener {
    void onInitLedger(Ledger ledger);
    void onReceiveMessage(String topic, String queue, int ledger, int count);
    void onPushMessage(String topic, int ledger, int count);
}
