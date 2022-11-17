package org.leopard.ledger;

public interface LedgerTrigger {
    void onAppend(int limit, Offset tail);
}
