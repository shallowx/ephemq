package org.meteor.proxy.management;

public interface ZookeeperPathConstants {
    String PROXIES = "/proxies";
    String PROXIES_IDS = PROXIES + "/ids";
    String PROXIES_ID = PROXIES_IDS + "/%s";
}
