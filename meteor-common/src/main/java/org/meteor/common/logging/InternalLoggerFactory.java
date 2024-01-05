package org.meteor.common.logging;

import org.meteor.common.util.ObjectUtil;

public abstract class InternalLoggerFactory {
    private static volatile InternalLoggerFactory defaultFactory;

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

    private static InternalLoggerFactory useJdkLoggerFactory(String name) {
        InternalLoggerFactory f = JdkLoggerFactory.INSTANCE;
        f.newLogger(name).debug("Using java.util.logging as the default logging framework");
        return f;
    }

    public static InternalLoggerFactory getDefaultFactory() {
        if (null == defaultFactory) {
            defaultFactory = newDefaultFactory(InternalLoggerFactory.class.getName());
        }
        return defaultFactory;
    }

    public static void setDefaultFactory(InternalLoggerFactory defaultFactory) {
        InternalLoggerFactory.defaultFactory = ObjectUtil.checkNotNull(defaultFactory, "defaultFactory");
    }


    public static InternalLogger getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    public static InternalLogger getLogger(String name) {
        return getDefaultFactory().newLogger(name);
    }

    protected abstract InternalLogger newLogger(String name);

}
