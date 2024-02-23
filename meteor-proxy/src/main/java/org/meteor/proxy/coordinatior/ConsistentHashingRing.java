package org.meteor.proxy.coordinatior;

import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class ConsistentHashingRing {
    private final NavigableMap<Integer, NavigableSet<String>> virtualNodes = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashFunction function = Hashing.murmur3_32();
    private final int virtualNodeSize;
    private final Set<String> nodes = new HashSet<>();

    public ConsistentHashingRing() {
        this(256);
    }

    public ConsistentHashingRing(int virtualNodeSize) {
        this.virtualNodeSize = virtualNodeSize;
    }

    public void insertNode(String node) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (!nodes.add(node)) {
                return;
            }
            for (int i = 0; i < virtualNodeSize; i++) {
                int hash = hashing(constructVirtualNodeName(node, i));
                virtualNodes.computeIfAbsent(hash, k -> new TreeSet<>()).add(node);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private int hashing(String key) {
        return function.hashUnencodedChars(key).asInt();
    }

    private String constructVirtualNodeName(String node, int index) {
        return node + "#" + index;
    }

    public void deleteNode(String node) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (!nodes.remove(node)) {
                return;
            }

            for (int i = 0; i < virtualNodeSize; i++) {
                int hash = hashing(constructVirtualNodeName(node, i));
                NavigableSet<String> navigates = virtualNodes.get(hash);
                if (navigates != null) {
                    navigates.remove(node);
                    if (navigates.isEmpty()) {
                        virtualNodes.remove(hash);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Set<String> route2Nodes(String key, int size) {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            if (virtualNodes.isEmpty() || size < 1) {
                return Collections.emptySet();
            }
            size = Math.min(size, nodes.size());
            LinkedHashSet<String> selectNodes = Sets.newLinkedHashSetWithExpectedSize(size);
            int hash = hashing(key);
            Map.Entry<Integer, NavigableSet<String>> tempEntry = virtualNodes.higherEntry(hash);
            for (int i = 0, limit = virtualNodes.size(); i < limit; i++) {
                if (tempEntry == null) {
                    tempEntry = virtualNodes.firstEntry();
                }

                for (String node : tempEntry.getValue()) {
                    if (selectNodes.add(node) && selectNodes.size() >= size) {
                        return selectNodes;
                    }
                }

                tempEntry = virtualNodes.higherEntry(tempEntry.getKey());
            }
            return selectNodes;
        } finally {
            readLock.unlock();
        }
    }
}
