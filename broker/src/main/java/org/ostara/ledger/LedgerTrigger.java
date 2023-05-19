package org.ostara.ledger;

import org.ostara.common.Offset;

public interface LedgerTrigger {
    void onAppend(int limit, Offset tail);
}
