package org.shallow.internal.atomic;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class DistributedAtomicValue<T> {

    private long preValue;
    private long postValue;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public AtomicValue<byte[]> get() {
        readLock.lock();
        try {
            byte[] preValueBytes = long2Bytes(preValue);
            byte[] postValueBytes = long2Bytes(postValue);
            return new MutableAtomicValue<>(preValueBytes, postValueBytes);
        }finally {
            readLock.unlock();
        }
    }

    public void trySet(Number newValue) {
        writeLock.lock();
        try {
            long v = newValue.longValue();
            preValue = v;
            postValue = v + 1;
        } finally {
            writeLock.unlock();
        }
    }

    public AtomicValue<byte[]> worker(Number newValue) {
        writeLock.lock();
        try {
            preValue += newValue.longValue();

            byte[] preValueBytes = long2Bytes(preValue);
            byte[] postValueBytes = long2Bytes((postValue = preValue + 1));

            return new MutableAtomicValue<>(preValueBytes, postValueBytes);
        } finally {
            writeLock.unlock();
        }
    }

    private byte[] long2Bytes(long newValue) {
        return new byte[]{
                (byte) ((newValue >> 56) & 0xFF),
                (byte) ((newValue >> 48) & 0xFF),
                (byte) ((newValue >> 40) & 0xFF),
                (byte) ((newValue >> 32) & 0xFF),
                (byte) ((newValue >> 24) & 0xFF),
                (byte) ((newValue >> 16) & 0xFF),
                (byte) ((newValue >> 8)  & 0xFF),
                (byte) (newValue & 0xFF)
        };
    }

    private static class MutableAtomicValue<T> implements AtomicValue<T> {

        T preValue;
        T postValue;

        public MutableAtomicValue(T preValue, T postValue) {
            this.preValue = preValue;
            this.postValue = postValue;
        }

        @Override
        public T preValue() {
            return preValue;
        }

        @Override
        public T postValue() {
            return postValue;
        }
    }
}
