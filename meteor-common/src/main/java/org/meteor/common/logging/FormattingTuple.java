package org.meteor.common.logging;

/**
 * This class represents a tuple consisting of a formatted message and an associated throwable.
 * It is used to capture the result of formatting operations, particularly in logging scenarios.
 */
public final class FormattingTuple {
    /**
     * The formatted message captured in this tuple.
     * This string represents the result of a message formatting operation,
     * typically used in logging scenarios.
     */
    private final String message;
    /**
     * The Throwable instance that is associated with a formatted message.
     * It represents any exception or error that may have occurred,
     * providing details about the nature of the issue.
     */
    private final Throwable throwable;

    /**
     * Constructs a FormattingTuple with the provided message and throwable.
     *
     * @param message   the formatted message
     * @param throwable the associated throwable, if any
     */
    FormattingTuple(String message, Throwable throwable) {
        this.message = message;
        this.throwable = throwable;
    }

    /**
     * Retrieves the formatted message.
     *
     * @return the formatted message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Retrieves the throwable associated with this formatting tuple.
     *
     * @return the associated throwable, or null if no throwable is associated.
     */
    public Throwable getThrowable() {
        return throwable;
    }
}
