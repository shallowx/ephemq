package org.meteor.support;

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
 * Represents a consistent hashing ring used to distribute load across multiple nodes.
 * This class supports the addition and removal of nodes and allows for key-based routing.
 */
public final class ConsistentHashingRing {
    /**
     * A navigable map that stores virtual nodes for a consistent hashing ring. The key is the hash
     * value of the virtual node and the value is a navigable set of node names associated with that hash.
     * The map ensures that the virtual nodes are maintained in a sorted order.
     */
    private final NavigableMap<Integer, NavigableSet<String>> virtualNodes = new TreeMap<>();
    /**
     * A read-write lock used to manage concurrent access to the consistent hashing ring.
     * Ensures thread safety when performing read and write operations on the nodes and virtual nodes.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * A lock used to ensure thread safety for write operations on the consistent hashing ring.
     * This lock is part of a read-write lock structure, allowing multiple readers but only one writer at a time.
     */
    private final Lock writeLock = lock.writeLock();
    /**
     * A lock used to ensure safe concurrent read access to the data structures managing the consistent hashing ring.
     */
    private final Lock readLock = lock.readLock();
    /**
     * A hash function implementation using Murmur3 32-bit algorithm.
     * This function is used for generating consistent hashes for keys.
     */
    private final HashFunction function = Hashing.murmur3_32_fixed();
    /**
     * The number of virtual nodes to be created for each physical node in the consistent hashing ring.
     * This provides a means to distribute the keys more evenly across the nodes, improving load balancing.
     * A higher number of virtual nodes tends to produce a more even distribution of load, but it
     * also increases the computation required when performing operations such as inserting and deleting nodes.
     */
    private final int virtualNodeSize;
    /**
     * A set containing real node identifiers in the consistent hashing ring.
     */
    private final Set<String> nodes = new HashSet<>();

    /**
     * Default constructor for the ConsistentHashingRing class.
     * <p>
     * Initializes a new instance of ConsistentHashingRing with a default number of virtual nodes.
     * The default value for the number of virtual nodes is set to 256.
     */
    public ConsistentHashingRing() {
        this(256);
    }

    /**
     * Constructor for the ConsistentHashingRing class that accepts the size of virtual nodes.
     *
     * @param virtualNodeSize the number of virtual nodes to be used in the consistent hashing ring
     */
    public ConsistentHashingRing(int virtualNodeSize) {
        this.virtualNodeSize = virtualNodeSize;
    }

    /**
     * Inserts a new node into the consistent hashing ring.
     *
     * @param node The identifier of the node to be inserted into the ring.
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
     * Generates a consistent hash for a given key.
     *
     * @param key The input string for which the hash needs to be generated.
     * @return The hash value as an integer.
     */
    private int hashing(String key) {
        return function.hashUnencodedChars(key).asInt();
    }

    /**
     * Creates a unique name for a virtual node by combining the given node name and an index.
     *
     * @param node the name of the physical node
     * @param index the index of the virtual node associated with the physical node
     * @return a unique name representing the virtual node
     */
    private String createVirtualNodeName(String node, int index) {
        return node + "#" + index;
    }

    /**
     * Removes the specified node from the consistent hashing ring and its virtual nodes.
     *
     * @param node the identifier of the node to be deleted from the ring
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
     * Routes the given key to a set of nodes based on the consistent hashing algorithm.
     *
     * @param key the key to be hashed and used for node selection
     * @param size the number of nodes to route to
     * @return a set of nodes that the key maps to
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
