package org.ostara.atomic;

public interface AtomicValue<T> {

    T preValue();

    T postValue();
}
