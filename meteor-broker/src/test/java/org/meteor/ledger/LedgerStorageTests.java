package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Assert;
import org.junit.Test;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.NetworkUtil;

import java.util.concurrent.CountDownLatch;

public class LedgerStorageTests {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerStorageTests.class);

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
                Assert.assertEquals(offset.getEpoch(), 0);
                Assert.assertEquals(offset.getIndex(), 1);
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

    @Test
    public void testSegmentBytes() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerTest());

        ByteBuf message = ByteBufUtil.string2Buf("test-append-record");
        Promise<Offset> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        storage.appendRecord(1, message, promise);

        long bytes = storage.segmentBytes();
        message.release();
        Assert.assertEquals(bytes, 0);
        storage.close(null);
    }

    static class LedgerTriggerTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
            Assert.assertEquals(ledgerId, 1);
            Assert.assertEquals(recordCount, 1);
            Assert.assertEquals(lasetOffset.getEpoch(), 0);
            Assert.assertEquals(lasetOffset.getIndex(), 1);
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }
}
