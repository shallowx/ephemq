package org.shallow.internal.atomic;

public interface AtomicValue<T> {

    T preValue();

    T postValue();
}
