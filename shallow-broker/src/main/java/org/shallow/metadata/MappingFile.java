package org.shallow.metadata;

import org.shallow.core.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class MappingFile {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MappingFile.class);

    private final BrokerConfig config;

    public MappingFile(BrokerConfig config) {
        this.config = config;
    }

    public void start() {

    }

    public void append2File(String path, String content) {

    }

    private String load(String path) {
        return null;
    }

    private boolean checkIsExists(String path) {
        return false;
    }

    private void newFile(String path) {

    }
}
