package org.shallow.internal;

import java.util.Properties;

public class MetaConfig {
    private final Properties config;

    public static MetaConfig exchange(Properties properties) {
        return new MetaConfig(properties);
    }

    private MetaConfig(Properties config) {
        this.config = config;
    }

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }
}
