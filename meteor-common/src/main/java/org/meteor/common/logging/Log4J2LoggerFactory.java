package org.meteor.common.logging;

import org.apache.logging.log4j.LogManager;

/**
 * Factory class for creating Log4J2-based {@link InternalLogger} instances.
 * <p>
 * This factory is a singleton, and the single instance can be accessed via the
 * {@code INSTANCE} constant. The factory creates new {@code InternalLogger} instances
 * using Log4J2's {@code LogManager}.
 */
public final class Log4J2LoggerFactory extends InternalLoggerFactory {
    /**
     * Singleton instance of {@link InternalLoggerFactory} using Log4J2LoggerFactory.
     *
     * This provides a centralized access point for getting the single instance of
     * Log4J2LoggerFactory throughout the application.
     */
    public static final InternalLoggerFactory INSTANCE = new Log4J2LoggerFactory();

    /**
     * Creates a new Log4J2-based {@link InternalLogger} instance with the specified name.
     *
     * @param name the name of the logger to be created
     * @return a newly created {@link InternalLogger} instance
     */
    @Override
    public InternalLogger newLogger(String name) {
        return new Log4J2Logger(LogManager.getLogger(name));
    }
}
