package org.ostara.ledger

import io.netty.channel.Channel
import org.ostara.internal.config.ServerConfig
import org.ostara.remote.util.NetworkUtils
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

@Stepwise
@Subject(Ledger)
class LedgerTests extends Specification {

    def topic = "test"
    def queue = "test"
    def epoch = -1
    def ledgerId = 1
    def partitionId = 1
    Ledger ledger

    def setup() {
        ledger = new Ledger(ServerConfig.exchange(new Properties()), topic, partitionId, ledgerId, epoch)
    }

    def "can create ledger"() {
        expect:
        ledger != null
    }

    def "subscribe topic"() {
        given:
        def channel = Mock(Channel.class)
        ledger.subscribe(channel, topic, queue, -1 as short, epoch, -1, NetworkUtils.newImmediatePromise())

        cleanup:
        ledger == null
    }

}
