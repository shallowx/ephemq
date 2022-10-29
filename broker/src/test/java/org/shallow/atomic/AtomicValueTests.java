package org.shallow.atomic;

import org.junit.Assert;
import org.junit.Test;
import org.shallow.internal.atomic.DistributedAtomicInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AtomicValueTests {

    @Test
    public void testAtomicIntegerValue() {
        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger();

        Integer preValue = atomicValue.get().preValue();
        Assert.assertEquals(0, (long)preValue);


        atomicValue.trySet(1);
        Integer preSetValue = atomicValue.get().preValue();
        Assert.assertEquals(1, (long)preSetValue);

        Integer incrementPreValue = atomicValue.increment().preValue();
        Integer incrementPostValue = atomicValue.get().postValue();

        Assert.assertEquals(2, incrementPreValue.longValue());
        Assert.assertEquals(3, incrementPostValue.longValue());
    }

    @Test
    public void testThreadSafe() throws InterruptedException {
        List<Integer> result1 = new ArrayList<>();
        List<Integer> result2 = new ArrayList<>();

        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                Integer preValue = atomicValue.increment().preValue();
                result1.add(preValue);
            }

            latch.countDown();
        }, "thread-safe-1");

        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                Integer preValue = atomicValue.increment().preValue();
                result2.add(preValue);
            }

            latch.countDown();
        },"thread-safe-2");

        t1.start();
        t2.start();

        latch.await();

        result1.retainAll(result2);
        Assert.assertTrue(result1.isEmpty());
    }

    @Test
    public void testMultipleAtomicValue() {
        DistributedAtomicInteger atomicValue0 = new DistributedAtomicInteger();
        Integer incrementPreValue0 = atomicValue0.increment().preValue();

        Assert.assertEquals(1, incrementPreValue0.longValue());

        DistributedAtomicInteger atomicValue1 = new DistributedAtomicInteger();
        Integer incrementPreValue1 = atomicValue1.increment().preValue();

        Assert.assertEquals(1, incrementPreValue1.longValue());
    }
}
