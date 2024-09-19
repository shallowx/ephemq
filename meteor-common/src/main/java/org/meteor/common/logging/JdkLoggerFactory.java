package org.meteor.common.logging;


import java.util.logging.Logger;

/**
 * Factory class for creating instances of {@link JdkLogger} which use the JDK logging framework.
 * This class extends the {@link InternalLoggerFactory} and provides an implementation for
 * creating new logger instances.
 */
public class JdkLoggerFactory extends InternalLoggerFactory {
    /**
     * Singleton instance of the {@link InternalLoggerFactory} using the JDK logging framework.
     */
    public static final InternalLoggerFactory INSTANCE = new JdkLoggerFactory();

    /**
     * Creates a new logger instance with the specified name using the JDK logging framework.
     *
     * @param name the name of the logger to create
     * @return a new instance of {@link InternalLogger} configured with the specified name
     */
    @Override
    public InternalLogger newLogger(String name) {
        return new JdkLogger(Logger.getLogger(name));
    }
}
