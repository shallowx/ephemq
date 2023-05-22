package org.ostara.log;

import org.ostara.core.Config;
import org.ostara.management.Manager;

public class LogManager {
    private Config config;
    private Manager manager;

    public LogManager(Config config, Manager manager) {
        this.config = config;
        this.manager = manager;
    }
}
