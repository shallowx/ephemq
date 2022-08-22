package org.shallow.log;

public interface LedgerTrigger {
    void onAppend(int ledgerId, int limit, Offset tail);
}
