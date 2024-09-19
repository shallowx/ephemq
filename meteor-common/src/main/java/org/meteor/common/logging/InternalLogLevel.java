package org.meteor.common.logging;

/**
 * Represents the various log levels in the internal logging system.
 * <p>
 * This enum defines the following log levels:
 * <p>
 * - TRACE: Designates finer-grained informational events than the DEBUG.
 * - DEBUG: Designates fine-grained informational events that are most useful to debug an application.
 * - INFO: Designates informational messages that highlight the progress of the application at coarse-grained level.
 * - WARN: Designates potentially harmful situations.
 * - ERROR: Designates error events that might still allow the application to continue running.
 * <p>
 * This enum is frequently used in logging classes to determine the specific log level and to perform corresponding log operations.
 */
public enum InternalLogLevel {
    TRACE, DEBUG, INFO, WARN, ERROR
}
