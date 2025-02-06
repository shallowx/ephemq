package org.meteor.common.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

/**
 * A factory for creating SLF4J-based {@link InternalLogger} instances.
 * This class extends the {@link InternalLoggerFactory} to utilize the SLF4J logging framework.
 */
public class Slf4JLoggerFactory extends InternalLoggerFactory {

    /**
     * Constructs an instance of {@code Slf4JLoggerFactory}.
     *
     * @param failIfNOP a boolean indicating whether to fail if NOPLoggerFactory is found.
     *                  When set to true, an assertion is made and a NoClassDefFoundError
     *                  is thrown if the logger factory is an instance of NOPLoggerFactory.
     *                  This is designed to prevent the usage of unsupported NOPLoggerFactory.
     */
    Slf4JLoggerFactory(boolean failIfNOP) {
        assert failIfNOP;
        if (LoggerFactory.getILoggerFactory() instanceof NOPLoggerFactory) {
            throw new NoClassDefFoundError("NOPLoggerFactory not supported");
        }
    }

    /**
     * Wraps a given SLF4J Logger instance into an InternalLogger instance.
     *
     * @param logger the SLF4J Logger instance to be wrapped
     * @return an InternalLogger instance that represents the wrapped logger
     */
    static InternalLogger wrapLogger(Logger logger) {
        return logger instanceof LocationAwareLogger ?
                new LocationAwareSlf4JLogger((LocationAwareLogger) logger) : new Slf4JLogger(logger);
    }

    /**
     * Returns an instance of {@link InternalLoggerFactory} that performs a NOP (no-operation) check.
     * If the underlying logger factory is NOPLoggerFactory, this method throws an error.
     *
     * @return an instance of {@link InternalLoggerFactory} with a NOP check.
     */
    static InternalLoggerFactory getInstanceWithNopCheck() {
        return NopInstanceHolder.INSTANCE_WITH_NOP_CHECK;
    }

    /**
     * Creates a new {@link InternalLogger} instance using the specified name.
     * This method wraps the SLF4J logger obtained by the given name in an InternalLogger.
     *
     * @param name the name of the logger to create
     * @return a new {@link InternalLogger} instance
     */
    @Override
    public InternalLogger newLogger(String name) {
        return wrapLogger(LoggerFactory.getLogger(name));
    }

    private static final class NopInstanceHolder {
        private static final InternalLoggerFactory INSTANCE_WITH_NOP_CHECK = new Slf4JLoggerFactory(true);
    }
}
