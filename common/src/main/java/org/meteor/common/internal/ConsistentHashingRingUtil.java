package org.meteor.common.internal;

import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentHashingRingUtil {
    private final NavigableMap<Integer, NavigableSet<String>> virtualNodes = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashFunction function = Hashing.murmur3_32();
    private final int virtualNodeSize;
    private final Set<String> nodes = new HashSet<>();

    public ConsistentHashingRingUtil() {
        this(256);
    }

    public ConsistentHashingRingUtil(int virtualNodeSize) {
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
            if (!nodes.remove(nodes)) {
                return;
            }

            for (int i = 0; i < virtualNodeSize; i++) {
                int hash = hashing(constructVirtualNodeName(node, i));
                NavigableSet<String> navigables = virtualNodes.get(hash);
                if (navigables != null) {
                    navigables.remove(node);
                    if (navigables.isEmpty()) {
                        virtualNodes.remove(hash);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public List<String> route2Nodes(String key, int size) {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            if (virtualNodes.isEmpty() || size < 1) {
                return Collections.EMPTY_LIST;
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
                        return new ArrayList<>(selectNodes);
                    }
                }

                tempEntry = virtualNodes.higherEntry(tempEntry.getKey());
            }
            return new ArrayList<>(selectNodes);
        } finally {
            readLock.unlock();
        }
    }
}
