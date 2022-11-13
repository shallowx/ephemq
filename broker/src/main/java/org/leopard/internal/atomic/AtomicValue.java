package org.leopard.internal.atomic;

public interface AtomicValue<T> {

    T preValue();

    T postValue();
}
