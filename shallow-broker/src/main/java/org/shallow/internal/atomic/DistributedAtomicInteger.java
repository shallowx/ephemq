package org.shallow.internal.atomic;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;

import static org.shallow.util.ObjectUtil.isNull;

@ThreadSafe
@SuppressWarnings("rawtypes,unchecked")
public class DistributedAtomicInteger implements DistributedAtomicNumber<Integer> {

    private final DistributedAtomicValue<Integer> value;

    public DistributedAtomicInteger() {
        this.value = new DistributedAtomicValue<>();
    }

    @Override
    public AtomicValue<Integer> get() {
        return new AtomicInteger(value.get());
    }

    @Override
    public void trySet(Integer newValue) {
        value.trySet(newValue);
    }

    @Override
    public AtomicValue<Integer> increment() {
        AtomicValue worker = value.worker(1);
        return new AtomicInteger(worker);
    }

    @VisibleForTesting
    int bytesToValue(byte[] data) {
        if (isNull(data) || data.length == 0) {
            return 0;
        }

        ByteBuffer wrapper = ByteBuffer.wrap(data);
        return ((Number)wrapper.getLong()).intValue();
    }

    private class AtomicInteger implements AtomicValue<Integer> {
        private final AtomicValue<byte[]> bytes;

        public AtomicInteger(AtomicValue<byte[]> bytes) {
            this.bytes = bytes;
        }

        @Override
        public Integer preValue() {
            return bytesToValue(bytes.preValue());
        }

        @Override
        public Integer postValue() {
            return bytesToValue(bytes.postValue());
        }
    }
}
