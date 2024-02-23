package org.meteor.thread;

import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class MeteorThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_ID = new AtomicInteger();
    private final AtomicInteger nextId = new AtomicInteger();
    private final String prefix;
    private final boolean daemon;
    private final int priority;
    protected final ThreadGroup threadGroup;

    public MeteorThreadFactory(Class<?> poolType) {
        this(poolType, false, Thread.NORM_PRIORITY);
    }

    public MeteorThreadFactory(String poolName) {
        this(poolName, false, Thread.NORM_PRIORITY);
    }

    public MeteorThreadFactory(Class<?> poolType, boolean daemon) {
        this(poolType, daemon, Thread.NORM_PRIORITY);
    }

    public MeteorThreadFactory(String poolName, boolean daemon) {
        this(poolName, daemon, Thread.NORM_PRIORITY);
    }

    public MeteorThreadFactory(Class<?> poolType, int priority) {
        this(poolType, false, priority);
    }

    public MeteorThreadFactory(String poolName, int priority) {
        this(poolName, false, priority);
    }

    public MeteorThreadFactory(Class<?> poolType, boolean daemon, int priority) {
        this(toPoolName(poolType), daemon, priority);
    }

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

    public MeteorThreadFactory(String poolName, boolean daemon, int priority, ThreadGroup threadGroup) {
        ObjectUtil.checkNotNull(poolName, "Meteor thread pool name cannot be empty");
        if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException(
                    "Thread priority is actually [ " + priority + "], but the expectation in [1, 10]");
        }

        prefix = poolName + '-' + POOL_ID.incrementAndGet() + '-';
        this.daemon = daemon;
        this.priority = priority;
        this.threadGroup = threadGroup;
    }

    public MeteorThreadFactory(String poolName, boolean daemon, int priority) {
        this(poolName, daemon, priority, null);
    }

    @Override
    public Thread newThread(@Nonnull Runnable r) {
        Thread t = newThread(r, prefix + nextId.incrementAndGet());
        try {
            if (t.isDaemon() != daemon) {
                t.setDaemon(daemon);
            }

            if (t.getPriority() != priority) {
                t.setPriority(priority);
            }
        } catch (Exception ignored) {
            // Doesn't matter even if failed to set.
        }
        return t;
    }

    protected Thread newThread(Runnable r, String name) {
        return new FastThreadLocalThread(threadGroup, r, name);
    }
}
