package org.ephemq.proxy.support;

/**
 * This class contains constants for constructing ZooKeeper paths related to proxy configurations.
 * It provides structured paths used primarily for managing proxy nodes and their associated IDs
 * within a ZooKeeper ensemble.
 */
public final class ZookeeperProxyPathConstants {
    /**
     * The base path in ZooKeeper for storing proxy configurations.
     * It serves as the root node under which all proxy-related nodes
     * and configurations will be organized.
     */
    private static final String PROXIES = "/proxies";
    /**
     * Constant representing the ZooKeeper path for proxy IDs.
     * This path is used to manage the IDs of proxy nodes within a ZooKeeper ensemble.
     */
    public static final String PROXIES_IDS = PROXIES + "/ids";
    /**
     * This constant string represents the path in the ZooKeeper ensemble
     * that refers to a specific proxy ID. It is a formatted string where
     * the '%s' placeholder is intended to be replaced with the actual
     * proxy ID.
     */
    public static final String PROXIES_ID = PROXIES_IDS + "/%s";
}
