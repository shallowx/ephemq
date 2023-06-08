package org.ostara.benchmarks.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.openjdk.jmh.annotations.*;
import org.ostara.common.Offset;
import org.ostara.log.ledger.LedgerConfig;
import org.ostara.log.ledger.LedgerStorage;
import org.ostara.log.ledger.LedgerTrigger;
import org.ostara.remote.util.ByteBufUtils;
import org.ostara.remote.util.NetworkUtils;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class StorageBenchmark {

    private ByteBuf message;
    private LedgerStorage storage;
    private Promise<Offset> promise;
    @Setup
    public void setUp() {
        message = ByteBufUtils.string2Buf("test-append-record");
        promise = ImmediateEventExecutor.INSTANCE.newPromise();
        promise.addListener(f -> {});
        storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtils.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerBenchmarkTest());
    }

    @Benchmark
    public void testAppendRecord() throws Exception{
        storage.appendRecord(1, message, promise);
    }

    @TearDown
    public void clean() {
        message.release();
        storage.close(null);
    }

    static class LedgerTriggerBenchmarkTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {}

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }
}
