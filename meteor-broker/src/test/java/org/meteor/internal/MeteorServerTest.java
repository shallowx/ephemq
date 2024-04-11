package org.meteor.internal;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.meteor.config.ServerConfig;
import org.meteor.remoting.DefaultSocketServer;
import org.meteor.support.DefaultMeteorManager;

public class MeteorServerTest {
    @Test
    public void testStartAndStop() throws Exception {
        ServerConfig config = new ServerConfig(new Properties());
        DefaultSocketServer server = new DefaultSocketServer(config, new DefaultMeteorManager());
        server.start();
        TimeUnit.SECONDS.sleep(1);
        server.shutdown();
    }
}
