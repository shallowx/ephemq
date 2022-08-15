package org.shallow.metadata.atomic;

import com.google.common.annotations.VisibleForTesting;
import org.shallow.metadata.sraft.SRaftProcessController;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class DistributedAtomicValue<T> {

    private volatile long value;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<DistributedAtomicValue> updater = AtomicLongFieldUpdater.newUpdater(DistributedAtomicValue.class, "value");

    public DistributedAtomicValue(SRaftProcessController controller) {
        this.value = controller.distributedValue();
    }

    public AtomicValue<byte[]> get() {
        byte[] preValueBytes = valueToBytes(value);
        return new MutableAtomicValue<>(preValueBytes, null);
    }

    public void trySet(T newValue) {
        updater.compareAndSet(this, value, (long)newValue);
    }

    public AtomicValue<byte[]> worker(T newValue) {
        byte[] preValueBytes = valueToBytes(value);

        updater.compareAndSet(this, value, (value + ((Number) newValue).longValue()));
        long postValue = updater.get(this);
        byte[] postValueBytes = valueToBytes(postValue);

        return new MutableAtomicValue<>(preValueBytes, postValueBytes);
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
