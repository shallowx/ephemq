package org.ostara.atomic;

public interface DistributedAtomicNumber<T> {

    AtomicValue<T> get();

    void trySet(T newValue);

    AtomicValue<T> increment();
}
