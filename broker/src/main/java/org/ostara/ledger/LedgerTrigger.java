package org.ostara.ledger;

public interface LedgerTrigger {
    void onAppend(int limit, Offset tail);
}
