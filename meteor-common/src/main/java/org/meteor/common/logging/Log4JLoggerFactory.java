package org.meteor.common.logging;

import org.apache.log4j.Logger;

/**
 * A factory class for creating instances of {@link InternalLogger} that utilize the Log4J logging framework.
 * This class implements the Singleton design pattern to ensure a single instance
 * of the Log4JLoggerFactory is used throughout the application.
 */
public class Log4JLoggerFactory extends InternalLoggerFactory {
    /**
     * A singleton instance of {@link InternalLoggerFactory}, specifically configured to use
     * Log4J as the underlying logging framework.
     * <p>
     * This factory instance provides methods to create loggers that utilize the Log4J framework
     * for logging operations.
     */
    public static final InternalLoggerFactory INSTANCE = new Log4JLoggerFactory();

    /**
     * Creates a new {@link InternalLogger} instance using the Log4J logging framework.
     *
     * @param name the name of the logger to be created
     * @return an instance of {@link InternalLogger} configured with the specified logger name
     */
    @Override
    public InternalLogger newLogger(String name) {
        return new Log4JLogger(Logger.getLogger(name));
    }
}
