package org.meteor.ledger;

import org.meteor.common.message.Offset;

public interface LedgerTrigger {
    void onAppend(int ledgerId, int recordCount, Offset lasetOffset);
    void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset);
}
