package org.meteor.ledger;

import org.meteor.common.message.Offset;

/**
 * Interface for handling ledger operations.
 * <p>
 * LedgerTrigger defines methods that are invoked on key ledger events,
 * such as appending records and releasing portions of the ledger.
 */
public interface LedgerTrigger {
    /**
     * Invoked when records are appended to the ledger.
     *
     * @param ledgerId    the ID of the ledger to which records are appended.
     * @param recordCount the number of records appended to the ledger.
     * @param lastOffset  the offset of the last appended record.
     */
    void onAppend(int ledgerId, int recordCount, Offset lastOffset);

    /**
     * Invoked when a portion of the ledger is released.
     *
     * @param ledgerId the identifier of the ledger from which a portion is being released
     * @param oldHeadOffset the previous head offset before the release
     * @param newHeadOffset the new head offset after the release
     */
    void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset);
}
