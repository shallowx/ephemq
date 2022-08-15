package org.shallow.metadata.atomic;

public interface AtomicValue<T> {

    T preValue();

    T postValue();
}
