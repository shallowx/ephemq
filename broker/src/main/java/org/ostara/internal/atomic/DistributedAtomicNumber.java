package org.ostara.internal.atomic;

public interface DistributedAtomicNumber<T> {

    AtomicValue<T> get();

    void trySet(T newValue);

    AtomicValue<T> increment();
}
