package org.ostara.ledger

import io.netty.util.concurrent.Promise
import org.ostara.internal.config.ServerConfig
import org.ostara.remote.util.ByteBufUtils
import org.ostara.remote.util.NetworkUtils
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

import java.nio.charset.StandardCharsets

@Stepwise
@Subject(Storage)
class StorageTests extends Specification {

    Storage storage
    Promise<Offset> promise = NetworkUtils.newImmediatePromise()
    def topic = "test"
    def queue = "test"
    def version = -1
    def payload = ByteBufUtils.byte2Buf("testWriteBuf".getBytes(StandardCharsets.UTF_8));

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

    def "append message."() {
        given:
        storage.append(topic, queue, version as short, payload, promise)

        cleanup:
        ByteBufUtils.release(payload);
    }

    def "locate cursor."() {
        given:
        storage.append(topic, queue, version as short, payload, promise)

        def offset = new Offset(-1, 1)
        def cursor = storage.locateCursor(offset)

        expect:
        !cursor.hashNext()
    }

}
