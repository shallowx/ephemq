package org.ephemq.dispatch;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.WeakHashMap;
import java.util.function.Function;

public class AbstractHandlerTests {

    @Test
    public void testGetCounts() {
        EventExecutor executor = new DefaultEventExecutor(new DefaultThreadFactory("AbstractHandlerTests"));
        TestHandler testHandler = new TestHandler(executor);

        Assertions.assertNotNull(testHandler);
        Assert.assertEquals(1, testHandler.getCounts(new EventExecutor[]{executor}, null)[0]);
    }

    @Test
    public void testApply() {
        EventExecutor executor = new DefaultEventExecutor(new DefaultThreadFactory("AbstractHandlerTests"));
        TestHandler testHandler = new TestHandler(executor);

        Assertions.assertNotNull(testHandler);
        Assert.assertEquals("test", testHandler.apply().apply(executor));
    }

    static class TestHandler extends AbstractHandler<String, String> {

        /**
         * Constructs an AbstractHandler with the specified EventExecutor.
         *
         * @param executor the executor to be used for dispatching events.
         */
        public TestHandler(EventExecutor executor) {
            super(executor);
        }

        @Override
        int[] getCounts(EventExecutor[] executors, WeakHashMap<String, Integer> handlers) {
            return new int[]{1};
        }

        @Override
        Function<EventExecutor, String> apply() {
            return r -> "test";
        }
    }
}
