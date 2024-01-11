package org.meteor.internal;

import org.junit.Test;
import org.meteor.config.ServerConfig;
import org.meteor.coordinatior.DefaultCoordinator;
import org.meteor.remoting.DefaultSocketServer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MeteorServerTests {
    @Test
    public void testStartAndStop() throws Exception {
        ServerConfig config = new ServerConfig(new Properties());
        DefaultSocketServer server = new DefaultSocketServer(config, new DefaultCoordinator());
        server.start();
        TimeUnit.SECONDS.sleep(1);
        server.shutdown();
    }
}
