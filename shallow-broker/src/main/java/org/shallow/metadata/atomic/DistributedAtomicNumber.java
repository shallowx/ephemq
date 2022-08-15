package org.shallow.metadata.atomic;

public interface DistributedAtomicNumber<T> {

    AtomicValue<T> get();

    void trySet(T newValue);

    AtomicValue<T> increment();
}
