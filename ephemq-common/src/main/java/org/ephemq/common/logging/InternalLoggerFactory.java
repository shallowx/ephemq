package org.ephemq.common.logging;

import org.ephemq.common.util.ObjectUtil;

/**
 * Abstract factory class for creating instances of {@link InternalLogger}.
 * This class follows the Singleton design pattern to ensure a single instance
 * of the default logger factory is used throughout the application.
 */
public abstract class InternalLoggerFactory {
    /**
     * Holds the default instance of the {@code InternalLoggerFactory}.
     * This field is initialized with a specific logging framework and can be updated
     * using the {@code setDefaultFactory} method.
     */
    private static volatile InternalLoggerFactory defaultFactory;

    /**
     * Creates a new default InternalLoggerFactory based on available logging frameworks.
     * This method tries to use SLF4J, Log4J2, Log4J, and JDK Logging in that order.
     *
     * @param name the name of the logger
     * @return an instance of InternalLoggerFactory based on the first available logging framework
     */
    @SuppressWarnings("UnusedCatchParameter")
    private static InternalLoggerFactory newDefaultFactory(String name) {
        InternalLoggerFactory f = useSlf4JLoggerFactory(name);
        if (null != f) {
            return f;
        }

        f = useLog4J2LoggerFactory(name);
        if (null != f) {
            return f;
        }

        f = useLog4JLoggerFactory(name);
        if (null != f) {
            return f;
        }

        return useJdkLoggerFactory(name);
    }

    /**
     * Attempts to initialize and return an instance of Slf4JLoggerFactory.
     * In case of any linkage error or exception during the initialization, it returns null.
     *
     * @param name the name of the logger
     * @return an instance of InternalLoggerFactory if SLF4J is available; null otherwise
     */
    private static InternalLoggerFactory useSlf4JLoggerFactory(String name) {
        try {
            InternalLoggerFactory f = Slf4JLoggerFactory.getInstanceWithNopCheck();
            f.newLogger(name).debug("Using SLF4J as the default logging framework");
            return f;
        } catch (LinkageError ignore) {
            return null;
        } catch (Exception ignore) {
            // We catch Exception and not ReflectiveOperationException as we still support java 6
            return null;
        }
    }

    /**
     * Attempts to use Log4J2 as the default logging framework.
     *
     * @param name the name of the logger
     * @return the Log4J2LoggerFactory instance if Log4J2 is available, otherwise null
     */
    private static InternalLoggerFactory useLog4J2LoggerFactory(String name) {
        try {
            InternalLoggerFactory f = Log4J2LoggerFactory.INSTANCE;
            f.newLogger(name).debug("Using Log4J2 as the default logging framework");
            return f;
        } catch (LinkageError ignore) {
            return null;
        } catch (Exception ignore) {
            // We catch Exception and not ReflectiveOperationException as we still support java 6
            return null;
        }
    }

    /**
     * Tries to use Log4J as the logging framework by creating and returning an instance of
     * Log4JLoggerFactory. This method will catch and suppress any errors or exceptions
     * that occur during the instantiation process.
     *
     * @param name the name of the logger to be created and used for logging.
     * @return an instance of InternalLoggerFactory representing the Log4J logger factory,
     * or null if any error or exception occurs during instantiation.
     */
    private static InternalLoggerFactory useLog4JLoggerFactory(String name) {
        try {
            InternalLoggerFactory f = Log4JLoggerFactory.INSTANCE;
            f.newLogger(name).debug("Using Log4J as the default logging framework");
            return f;
        } catch (LinkageError ignore) {
            return null;
        } catch (Exception ignore) {
            // We catch Exception and not ReflectiveOperationException as we still support java 6
            return null;
        }
    }

    /**
     * Configures and uses the JDK logging framework (`java.util.logging`) as the default logging framework.
     *
     * @param name the name of the logger to be created
     * @return an instance of {@code InternalLoggerFactory} configured to use the JDK logging framework
     */
    private static InternalLoggerFactory useJdkLoggerFactory(String name) {
        InternalLoggerFactory f = JdkLoggerFactory.INSTANCE;
        f.newLogger(name).debug("Using java.util.logging as the default logging framework");
        return f;
    }

    /**
     * Returns the default {@code InternalLoggerFactory} instance. If the default factory is not set,
     * this method initializes it using the {@code newDefaultFactory} method.
     *
     * @return the default {@code InternalLoggerFactory} instance
     */
    public static InternalLoggerFactory getDefaultFactory() {
        if (null == defaultFactory) {
            defaultFactory = newDefaultFactory(InternalLoggerFactory.class.getName());
        }
        return defaultFactory;
    }

    /**
     * Sets the default {@link InternalLoggerFactory} to be used.
     *
     * @param defaultFactory the new default logger factory. Must not be null.
     * @throws NullPointerException if the specified {@code defaultFactory} is null.
     */
    public static void setDefaultFactory(InternalLoggerFactory defaultFactory) {
        InternalLoggerFactory.defaultFactory = ObjectUtil.checkNotNull(defaultFactory, "defaultFactory");
    }


    /**
     * Retrieves an instance of InternalLogger for the specified class.
     *
     * @param clazz the class for which the logger is requested
     * @return an instance of InternalLogger associated with the given class
     */
    public static InternalLogger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * Retrieves an instance of {@code InternalLogger} with the specified name.
     *
     * @param name the name of the logger to retrieve
     * @return an instance of {@code InternalLogger} configured with the specified name
     */
    public static InternalLogger getLogger(String name) {
        return getDefaultFactory().newLogger(name);
    }

    /**
     * Creates a new logger instance with the specified name.
     *
     * @param name the name identifier for the logger
     * @return an instance of InternalLogger associated with the given name
     */
    protected abstract InternalLogger newLogger(String name);

}
