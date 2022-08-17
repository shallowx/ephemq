package org.shallow.metadata.atomic;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
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
            byte[] preValueBytes = valueToBytes(preValue);
            byte[] postValueBytes = valueToBytes(postValue);
            return new MutableAtomicValue<>(preValueBytes, postValueBytes);
        }finally {
            readLock.unlock();
        }
    }

    public void trySet(Number newValue) {
        writeLock.lock();
        try {
            preValue = newValue.longValue();
        } finally {
            writeLock.unlock();
        }
    }

    public AtomicValue<byte[]> worker(Number newValue) {
        writeLock.lock();
        try {
            preValue += newValue.longValue();

            byte[] preValueBytes = valueToBytes(preValue);
            byte[] postValueBytes = valueToBytes((postValue = preValue + 1));

            return new MutableAtomicValue<>(preValueBytes, postValueBytes);
        } finally {
            writeLock.unlock();
        }
    }

    @VisibleForTesting
    private byte[] valueToBytes(long newValue) {
        byte[] bytes = longToBytes(newValue);
        ByteBuffer wrapper = ByteBuffer.wrap(bytes);
        wrapper.putLong(newValue);

        return bytes;
    }

    private byte[] longToBytes(long newValue) {
       return new byte[] {
                (byte) newValue,
                (byte) (newValue >> 8),
                (byte) (newValue >> 16),
                (byte) (newValue >> 24),
                (byte) (newValue >> 32),
                (byte) (newValue >> 40),
                (byte) (newValue >> 48),
                (byte) (newValue >> 56)};
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
