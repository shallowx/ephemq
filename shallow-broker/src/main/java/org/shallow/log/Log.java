package org.shallow.log;

import org.shallow.metadata.management.ManagementListener;

public class Log implements ManagementListener {

    private int ledgerId;

    public Log() {
    }

    public int getLedgerId() {
        return ledgerId;
    }

    @Override
    public void initLog() {

    }
}
