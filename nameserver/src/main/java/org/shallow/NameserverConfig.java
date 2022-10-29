package org.shallow;

import java.util.Properties;

public class NameserverConfig {

    private final Properties config;

    public static NameserverConfig exchange(Properties properties) {
        return new NameserverConfig(properties);
    }

    private NameserverConfig(Properties config) {
        this.config = config;
    }

}
