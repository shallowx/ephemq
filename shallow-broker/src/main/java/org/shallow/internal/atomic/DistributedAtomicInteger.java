package org.shallow.internal.atomic;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
@SuppressWarnings("rawtypes,unchecked")
public class DistributedAtomicInteger implements DistributedAtomicNumber<Integer> {

    private final DistributedAtomicValue value;

    public DistributedAtomicInteger() {
        this.value = new DistributedAtomicValue();
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

    private int bytes2Integer(byte[] data) {
        long v = (long)(data[0] & 0xFF) << 56 |
                 (long)(data[1] & 0xFF) << 48 |
                 (long)(data[2] & 0xFF) << 40 |
                 (long)(data[3] & 0xFF) << 32 |
                 (long)(data[4] & 0xFF) << 24 |
                 (long)(data[5] & 0xFF) << 16 |
                 (long)(data[6] & 0xFF) << 8  |
                 (long) (data[7] & 0xFF);

        return (int) v;
    }

    private class AtomicInteger implements AtomicValue<Integer> {
        private final AtomicValue<byte[]> bytes;

        public AtomicInteger(AtomicValue<byte[]> bytes) {
            this.bytes = bytes;
        }

        @Override
        public Integer preValue() {
            return bytes2Integer(bytes.preValue());
        }

        @Override
        public Integer postValue() {
            return bytes2Integer(bytes.postValue());
        }
    }
}
