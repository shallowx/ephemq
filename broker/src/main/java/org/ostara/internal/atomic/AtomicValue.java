package org.ostara.internal.atomic;

public interface AtomicValue<T> {

    T preValue();

    T postValue();
}
