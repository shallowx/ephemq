package org.ostara.atomic


import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Subject

@Subject(DistributedAtomicValue)
@Stepwise
class DistributedAtomicValueTests extends Specification {

    def "can create AtomicValue of pre."() {
        given:
        def distributedAtomicValue = new DistributedAtomicValue();
        def value = distributedAtomicValue.get()

        expect:
        value.preValue() == (byte[]) [0, 0, 0, 0, 0, 0, 0, 0]
    }

    def "can create AtomicValue of post."() {
        given:
        def distributedAtomicValue = new DistributedAtomicValue()
        def value = distributedAtomicValue.get()

        expect:
        value.postValue() == (byte[]) [0, 0, 0, 0, 0, 0, 0, 0]
    }

    def "can create AtomicValue worker."() {
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
        value.preValue() == (byte[]) [0, 0, 0, 0, 0, 0, 0, 0]
    }

}
