package org.ostara.ledger

import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

@Subject(Offset)
@Stepwise
class OffsetTests extends Specification {

    def "return epoch."() {
        given:
        def offset = new Offset(-1, -1)

        expect:
        offset.epoch() == -1
    }

    def "return index."() {
        given:
        def offset = new Offset(-1, -1)

        expect:
        offset.index() == -1
    }

    def "before offset."() {
        given:
        def offset = new Offset(-1, -1)
        def before = new Offset(-1, 0)
        def result = offset.before(before)

        expect:
        result
    }

    def "after offset."() {
        given:
        def offset = new Offset(-1, -1)
        def before = new Offset(-1, 0)
        def result = offset.after(before)

        expect:
        !result
    }

    def "offset of."() {
        given:
        def offset = Offset.of(-1, -1)

        expect:
        offset == null
    }

}
