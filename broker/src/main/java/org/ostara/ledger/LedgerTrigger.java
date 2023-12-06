package org.ostara.ledger;

import org.ostara.common.Offset;

public interface LedgerTrigger {
    void onAppend(int ledgerId, int recordCount, Offset lasetOffset);

    void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset);
}
