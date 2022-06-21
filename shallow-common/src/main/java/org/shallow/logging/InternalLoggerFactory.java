package org.shallow.logging;

import org.shallow.ObjectUtil;

public abstract class InternalLoggerFactory {

    private static volatile InternalLoggerFactory defaultFactory;

    @SuppressWarnings("UnusedCatchParameter")
    private static InternalLoggerFactory newDefaultFactory(String name) {
        InternalLoggerFactory f = useSlf4JLoggerFactory(name);
        if (f != null) {
            return f;
        }

        f = useLog4J2LoggerFactory(name);
        if (f != null) {
            return f;
        }

        f = useLog4JLoggerFactory(name);
        if (f != null) {
            return f;
        }

        return useJdkLoggerFactory(name);
    }

    private static InternalLoggerFactory useSlf4JLoggerFactory(String name) {
        try {
            InternalLoggerFactory f = Slf4JLoggerFactory.getInstanceWithNopCheck();
            f.newInstance(name).debug("Using SLF4J as the default logging framework");
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
            f.newInstance(name).debug("Using Log4J2 as the default logging framework");
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
            f.newInstance(name).debug("Using Log4J as the default logging framework");
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
        f.newInstance(name).debug("Using java.util.logging as the default logging framework");
        return f;
    }

    public static InternalLoggerFactory getDefaultFactory() {
        if (defaultFactory == null) {
            defaultFactory = newDefaultFactory(InternalLoggerFactory.class.getName());
        }
        return defaultFactory;
    }

    public static InternalLogger getInstance(String name) {
        return getDefaultFactory().newInstance(name);
    }

    protected abstract InternalLogger newInstance(String name);

}
