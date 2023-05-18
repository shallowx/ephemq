package org.ostara.atomic


import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

@Subject(DistributedAtomicInteger)
@Stepwise
class DistributedAtomicIntegerTests extends Specification {

    def "should be return 0 by get()."() {
        given:
        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger()
        Integer preValue = atomicValue.get().preValue()

        expect:
        preValue == 0
    }

    def "should be return 1 by trySet()."() {
        given:
        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger()
        atomicValue.trySet(1)

        Integer preValue = atomicValue.get().preValue()

        expect:
        preValue == 1
    }

    def "should be return 1 by increment pre()."() {
        given:
        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger()
        Integer preValue = atomicValue.increment().preValue()

        expect:
        preValue == 1
    }

    def "should be return 2 by increment post()."() {
        given:
        DistributedAtomicInteger atomicValue = new DistributedAtomicInteger()
        Integer postValue = atomicValue.increment().postValue()

        expect:
        postValue == 2
    }
}
