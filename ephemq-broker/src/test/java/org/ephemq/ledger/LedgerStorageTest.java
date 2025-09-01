package org.ephemq.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Offset;
import org.ephemq.remote.util.ByteBufUtil;
import org.ephemq.remote.util.NetworkUtil;

import java.util.concurrent.CountDownLatch;

public class LedgerStorageTest {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerStorageTest.class);

    /**
     * Test method for appending a record to the {@link LedgerStorage}.
     *
     * @throws Exception if any error occurs during the test execution.
     */
    @Test
    public void testAppendRecord() throws Exception {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerTest());

        ByteBuf message = ByteBufUtil.string2Buf("test-append-record");
        CountDownLatch latch = new CountDownLatch(1);
        Promise<Offset> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        promise.addListener(f -> {
            if (f.isSuccess()) {
                Offset offset = (Offset) f.get();
                Assert.assertEquals(0, offset.getEpoch());
                Assert.assertEquals(1, offset.getIndex());
            } else {
                Throwable cause = f.cause();
                Assert.assertNotNull(cause);
            }
            logger.info("Call back successes");
            latch.countDown();
        });

        storage.appendRecord(1, message, promise);
        latch.await();
        message.release();
        storage.close(null);
    }

    /**
     * Tests the {@link LedgerStorage#segmentBytes()} method to verify that
     * the segment size is correctly computed after appending a record.
     * The test involves:
     * 1. Initializing a new instance of {@code LedgerStorage}
     * 2. Appending a sample record to the ledger
     * 3. Asserting that the segment byte count is zero after appending the record
     * 4. Releasing the byte buffer and closing the storage
     */
    @Test
    public void testSegmentBytes() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerTest());

        ByteBuf message = ByteBufUtil.string2Buf("test-append-record");
        Promise<Offset> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        storage.appendRecord(1, message, promise);

        long bytes = storage.segmentBytes();
        message.release();
        Assert.assertEquals(0, bytes);
        storage.close(null);
    }

    static class LedgerTriggerTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
            Assert.assertEquals(1, ledgerId);
            Assert.assertEquals(1, recordCount);
            Assert.assertEquals(0, lasetOffset.getEpoch());
            Assert.assertEquals(1, lasetOffset.getIndex());
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }
}
