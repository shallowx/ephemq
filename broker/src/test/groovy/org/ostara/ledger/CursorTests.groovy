package org.ostara.ledger

import io.netty.buffer.PooledByteBufAllocator
import org.ostara.internal.config.ServerConfig
import org.ostara.remote.util.ByteBufUtils
import org.ostara.remote.util.NetworkUtils
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

import java.nio.charset.StandardCharsets

@Subject(Cursor)
@Stepwise
class CursorTests extends Specification {

    def topic = "test-topic";
    def queue = "test-queue";
    def payload = ByteBufUtils.byte2Buf("testWriteBuf".getBytes(StandardCharsets.UTF_8));
    def bytes = topic.length() + queue.length() + 26 + payload.readableBytes()
    def segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0))
    Storage storage

    def setup() {
        def executor = NetworkUtils.newEventExecutorGroup(1, "test").next()
        def config = ServerConfig.exchange(new Properties())
        storage = new Storage(executor, 1, config, -1, new LedgerTrigger() {
            @Override
            void onAppend(int limit, Offset tail) {
                println(String.format("limit=%d, offset=%s", limit, tail))
            }
        })
    }

    def "skip 2 tail"() {
        given:
        def cursor = new Cursor(storage, segment, -1)
        def tail = cursor.skip2Tail()

        expect:
        !tail.hashNext()
    }

    def "has next"() {
        given:
        def cursor = new Cursor(storage, segment, -1)
        def tail = cursor.hashNext()

        expect:
        tail
    }

    def "clone cursor"() {
        given:
        def cursor = new Cursor(storage, segment, -1)
        def clone = cursor.clone()

        expect:
        clone.hashNext()
    }

}
