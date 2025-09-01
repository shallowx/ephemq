package org.ephemq.ledger;

/**
 * The LogState enum represents the various states that a log can be in during its lifecycle.
 * <p>
 * APPENDABLE: The log is open and new entries can be appended.
 * <p>
 * SYNCHRONIZING: The log is currently being synchronized with other systems or logs to ensure consistency.
 * <p>
 * MIGRATING: The log is in the process of being moved or migrated from one storage medium to another.
 * <p>
 * CLOSED: The log is closed and no further entries can be appended.
 */
public enum LogState {
    APPENDABLE, SYNCHRONIZING, MIGRATING, CLOSED
}
