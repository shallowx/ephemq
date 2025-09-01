package org.ephemq.proxy.support;

import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implements a consistent hashing ring, a data structure that maps keys to nodes in a way that aims
 * to evenly distribute keys among nodes and minimize reassignments when nodes are added or removed.
 * It uses virtual nodes to achieve a more balanced key distribution.
 */
final class ConsistentHashingRing {
    /**
     * A navigable map that holds virtual nodes for consistent hashing.
     * Each virtual node is represented by an integer key, mapping to
     * a navigable set of node names (as strings). This map is used to
     * efficiently distribute keys across the nodes in a consistent hash ring.
     */
    private final NavigableMap<Integer, NavigableSet<String>> virtualNodes = new TreeMap<>();
    /**
     * A lock used for synchronizing read and write operations on the consistent hashing ring.
     * Ensures thread-safe access to the ring structure, preventing concurrent modification issues.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * A lock used to control write access to the resources in the consistent hashing ring.
     * Ensures that only one thread can modify the data structure at a time to maintain consistency.
     */
    private final Lock writeLock = lock.writeLock();
    /**
     * The lock used for controlling read access to the consistent hashing ring.
     * This ensures that multiple threads can read from the ring concurrently,
     * but exclusive access is required for write operations.
     */
    private final Lock readLock = lock.readLock();
    /**
     * A Murmur3 32-bit hash function used for hashing node keys in the consistent hashing ring.
     * This hash function ensures a good distribution of hash values, which helps in evenly
     * distributing nodes and managing data placement within the hash ring.
     */
    private final HashFunction function = Hashing.murmur3_32_fixed();
    /**
     * Represents the number of virtual nodes each physical node is mapped to in the consistent hashing ring.
     * A higher value of virtualNodeSize can result in a more balanced distribution of keys across the nodes.
     * It is a final variable, meaning its value is set once and cannot be modified.
     */
    private final int virtualNodeSize;
    /**
     * A set of physical nodes in the consistent hashing ring.
     */
    private final Set<String> nodes = new HashSet<>();

    /**
     * Default constructor that initializes a ConsistentHashingRing with a default number of virtual nodes.
     * The default number of virtual nodes is set to 256.
     */
    public ConsistentHashingRing() {
        this(256);
    }

    /**
     * Constructs a ConsistentHashingRing with a specified number of virtual nodes per physical node.
     *
     * @param virtualNodeSize the number of virtual nodes to be used for each physical node
     */
    public ConsistentHashingRing(int virtualNodeSize) {
        this.virtualNodeSize = virtualNodeSize;
    }

    /**
     * Inserts a physical node into the consistent hashing ring, and creates its corresponding virtual nodes.
     *
     * @param node The unique identifier of the physical node to be added to the consistent hashing ring.
     */
    public void insertNode(String node) {
        writeLock.lock();
        try {
            if (!nodes.add(node)) {
                return;
            }
            for (int i = 0; i < virtualNodeSize; i++) {
                int hash = hashing(createVirtualNodeName(node, i));
                virtualNodes.computeIfAbsent(hash, k -> new TreeSet<>()).add(node);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Computes the hash value for a given key using a specific hash function.
     *
     * @param key the input key to hash
     * @return the hash value as an integer
     */
    private int hashing(String key) {
        return function.hashUnencodedChars(key).asInt();
    }

    /**
     * Creates a virtual node name by combining the provided node identifier with an index.
     *
     * @param node  the base node identifier
     * @param index the index to append to the node name for differentiation
     * @return a unique virtual node name in the format "node#index"
     */
    private String createVirtualNodeName(String node, int index) {
        return node + "#" + index;
    }

    /**
     * Deletes a node from the consistent hashing ring.
     *
     * @param node The identifier of the node to be deleted.
     */
    public void deleteNode(String node) {
        writeLock.lock();
        try {
            if (!nodes.remove(node)) {
                return;
            }

            for (int i = 0; i < virtualNodeSize; i++) {
                int hash = hashing(createVirtualNodeName(node, i));
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

    /**
     * Routes the given key to a set of nodes in the consistent hashing ring.
     *
     * @param key  the key to be routed
     * @param size the number of nodes to be returned
     * @return a set of nodes that the key is routed to; an empty set if size is less than 1 or no nodes exist
     */
    public Set<String> route2Nodes(String key, int size) {
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
