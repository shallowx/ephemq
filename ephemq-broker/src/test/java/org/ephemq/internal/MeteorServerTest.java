package org.ephemq.internal;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.ephemq.config.ServerConfig;
import org.ephemq.remoting.DefaultSocketServer;
import org.ephemq.support.DefaultMeteorManager;

public class MeteorServerTest {
    /**
     * Tests the start and stop functionality of the DefaultSocketServer.
     * <p>
     * This method initializes a ServerConfig with default properties, creates an instance of DefaultSocketServer
     * with the configuration and a DefaultMeteorManager, and performs the following:
     * 1. Starts the server.
     * 2. Waits for 1 second to simulate some server runtime.
     * 3. Shuts down the server.
     *
     * @throws Exception if any error occurs during server start, sleep, or shutdown operations.
     */
    @Test
    public void testStartAndStop() throws Exception {
        ServerConfig config = new ServerConfig(new Properties());
        DefaultSocketServer server = new DefaultSocketServer(config, new DefaultMeteorManager());
        server.start();
        TimeUnit.SECONDS.sleep(1);
        server.shutdown();
    }
}
