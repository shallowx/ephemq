package org.ostara.atomic


import org.ostara.internal.atomic.DistributedAtomicValue
import spock.lang.Specification

class DistributedAtomicValueTests extends Specification {

    def "can create AtomicValue pre"() {
        given:
        def distributedAtomicValue = new DistributedAtomicValue();
        def value = distributedAtomicValue.get()

        expect:

        value.preValue() == [0, 0, 0, 0, 0, 0, 0, 0]
    }

    def "can create AtomicValue post"() {
        given:
        def distributedAtomicValue = new DistributedAtomicValue()
        def value = distributedAtomicValue.get()

        expect:

        value.postValue() == [0, 0, 0, 0, 0, 0, 0, 0]
    }

    def "can create AtomicValue worker"() {
        given:
        def distributedAtomicValue = new DistributedAtomicValue()
        def number = new Number() {
            @Override
            int intValue() {
                return 0
            }

            @Override
            long longValue() {
                return 0
            }

            @Override
            float floatValue() {
                return 0
            }

            @Override
            double doubleValue() {
                return 0
            }
        }

        def value = distributedAtomicValue.worker(number)

        expect:
        value.preValue() == [0, 0, 0, 0, 0, 0, 0, 0]
    }

}
