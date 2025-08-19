package org.meteor.support;

import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A factory for creating threads with customized settings such as name prefix, daemon status,
 * and priority. It ensures that threads created by this factory have unique names for easier identification.
 * <p>
 * This class implements the ThreadFactory interface, providing a way to customize thread creation
 * for executors and other concurrent utilities.
 */
class MeteorThreadFactory implements ThreadFactory {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorThreadFactory.class);
    /**
     * A static counter to generate unique identifiers for thread pools.
     * It is used to ensure that each thread pool created by the MeteorThreadFactory
     * has a distinct identifier for tracking and management purposes.
     */
    private static final AtomicInteger POOL_ID = new AtomicInteger();
    /**
     * The thread group to which the new threads created by this factory will belong.
     * This allows grouping related threads together for managing their execution.
     */
    protected final ThreadGroup threadGroup;
    /**
     * An atomic counter used to assign unique IDs to newly created threads.
     * This counter ensures that each thread created by the factory has a distinct, incrementing ID.
     */
    private final AtomicInteger nextId = new AtomicInteger();
    /**
     * The prefix used for naming threads created by this factory.
     * It is composed of the pool name and a unique pool identifier followed by a dash.
     */
    private final String prefix;
    /**
     * Indicates whether the threads created by this factory are daemon threads.
     */
    private final boolean daemon;
    /**
     * Priority level assigned to the threads created by the MeteorThreadFactory.
     * It represents the thread priority from a range typically defined by
     * constants from the Thread class (e.g., Thread.MIN_PRIORITY to Thread.MAX_PRIORITY).
     * <p>
     * This value is critical as it impacts the scheduling order of the threads
     * managed by the factory, ensuring certain threads can be more or less
     * responsive based on their assigned priority.
     */
    private final int priority;

    /**
     * Constructs a new MeteorThreadFactory.
     *
     * @param poolType the class type of the pool for which this thread factory is being created. This determines the base name for the threads created by this factory.
     */
    public MeteorThreadFactory(Class<?> poolType) {
        this(poolType, false, Thread.NORM_PRIORITY);
    }

    /**
     * Constructs a new MeteorThreadFactory with the specified pool name.
     *
     * @param poolName the name of the thread pool
     */
    public MeteorThreadFactory(String poolName) {
        this(poolName, false, Thread.NORM_PRIORITY);
    }

    /**
     * Constructs a new {@code MeteorThreadFactory} with the specified pool type and daemon flag.
     *
     * @param poolType the type of the pool for which threads will be created
     * @param daemon whether or not the threads created by this factory will be daemon threads
     */
    public MeteorThreadFactory(Class<?> poolType, boolean daemon) {
        this(poolType, daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Constructs a MeteorThreadFactory with the given pool name and daemon setting.
     *
     * @param poolName the name of the thread pool, cannot be empty
     * @param daemon whether or not the threads created by this factory should be daemon threads
     */
    public MeteorThreadFactory(String poolName, boolean daemon) {
        this(poolName, daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Constructs a new MeteorThreadFactory with the specified pool type and priority.
     *
     * @param poolType the class type of the thread pool
     * @param priority the priority for the threads created by this factory
     */
    public MeteorThreadFactory(Class<?> poolType, int priority) {
        this(poolType, false, priority);
    }

    /**
     * Constructs a MeteorThreadFactory with the specified pool name and priority.
     *
     * @param poolName the name of the thread pool
     * @param priority the priority of the threads created by this factory
     */
    public MeteorThreadFactory(String poolName, int priority) {
        this(poolName, false, priority);
    }

    /**
     * Constructor that initializes the MeteorThreadFactory with the specified pool type, daemon status, and priority.
     *
     * @param poolType the class type of the pool
     * @param daemon whether the threads should be daemon threads
     * @param priority the priority of the threads
     */
    public MeteorThreadFactory(Class<?> poolType, boolean daemon, int priority) {
        this(toPoolName(poolType), daemon, priority);
    }

    /**
     * Constructs a new MeteorThreadFactory with the specified settings.
     *
     * @param poolName    the name prefix for each thread created by this factory; must not be null.
     * @param daemon      if true, the new threads will be created as daemon threads.
     * @param priority    the priority for the new threads; must be between Thread.MIN_PRIORITY and Thread.MAX_PRIORITY.
     * @param threadGroup the thread group for the new threads; can be null.
     *
     * @throws IllegalArgumentException if the priority is not within the valid range.
     */
    public MeteorThreadFactory(String poolName, boolean daemon, int priority, ThreadGroup threadGroup) {
        ObjectUtil.checkNotNull(poolName, "Meteor thread pool name cannot be empty");
        if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException(String.format("Thread priority[%s] is actually, but the expectation in [1, 10]", priority));
        }

        prefix = poolName + '-' + POOL_ID.incrementAndGet() + '-';
        this.daemon = daemon;
        this.priority = priority;
        this.threadGroup = threadGroup;
    }

    /**
     * Constructs a new MeteorThreadFactory with the specified pool name, daemon status, and priority.
     *
     * @param poolName the name of the thread pool
     * @param daemon if true, threads created by this factory will be daemon threads
     * @param priority the priority of the threads created by this factory
     */
    public MeteorThreadFactory(String poolName, boolean daemon, int priority) {
        this(poolName, daemon, priority, null);
    }

    /**
     * Converts the provided class type of a pool to a standardized pool name.
     *
     * @param poolType the Class type of the pool, must not be null
     * @return the standardized pool name as a String
     */
    public static String toPoolName(Class<?> poolType) {
        ObjectUtil.checkNotNull(poolType, "Meteor thread pool type cannot be empty");

        String poolName = StringUtil.simpleClassName(poolType);
        switch (poolName.length()) {
            case 0 -> {
                return "unknown";
            }
            case 1 -> {
                return poolName.toLowerCase(Locale.US);
            }
            default -> {
                if (Character.isUpperCase(poolName.charAt(0)) && Character.isLowerCase(poolName.charAt(1))) {
                    return Character.toLowerCase(poolName.charAt(0)) + poolName.substring(1);
                } else {
                    return poolName;
                }
            }
        }
    }

    /**
     * Creates a new thread with specified configuration for running the provided Runnable.
     *
     * @param r The Runnable to be executed by the new thread.
     * @return The newly created and configured thread.
     */
    @Override
    public Thread newThread(@Nonnull Runnable r) {
        Thread t = new FastThreadLocalThread(r, prefix + nextId.incrementAndGet());
        try {
            if (t.isDaemon() != daemon) {
                t.setDaemon(daemon);
            }

            if (t.getPriority() != priority) {
                t.setPriority(priority);
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        return t;
    }
}
