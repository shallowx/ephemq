package org.ephemq.remote.invoke;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;

public class InvokeTests {


    private static final Logger log = LoggerFactory.getLogger(InvokeTests.class);

    @Test
    public void test() {
        TestVolatile testVolatile = new TestVolatile();
        testVolatile.set();
        int i = testVolatile.get();
        Assert.assertEquals(1, i);
    }

    static class TestVolatile {
        private volatile int completed;
        private static final VarHandle VALUE = MhUtil.findVarHandle(MethodHandles.lookup(), "completed", int.class);

        public void set() {
            VALUE.compareAndSet(this, 0, 1);
        }

        public int get() {
            return (int) VALUE.get(this);
        }
    }

    static class MhUtil {
        private MhUtil() {}

        public static VarHandle findVarHandle(MethodHandles.Lookup lookup,
                                              String name,
                                              Class<?> type) {
            return findVarHandle(lookup, lookup.lookupClass(), name, type);
        }

        public static VarHandle findVarHandle(MethodHandles.Lookup lookup,
                                              Class<?> recv,
                                              String name,
                                              Class<?> type) {
            try {
                return lookup.findVarHandle(recv, name, type);
            } catch (ReflectiveOperationException e) {
                throw new InternalError(e);
            }
        }

        public static MethodHandle findVirtual(MethodHandles.Lookup lookup,
                                               Class<?> refc,
                                               String name,
                                               MethodType type) {
            try {
                return lookup.findVirtual(refc, name, type);
            } catch (ReflectiveOperationException e) {
                throw new InternalError(e);
            }
        }
    }
}
