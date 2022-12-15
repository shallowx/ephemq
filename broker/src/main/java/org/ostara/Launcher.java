package org.ostara;

import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.thread.ShutdownHook;
import org.ostara.internal.SimpleServer;
import org.ostara.internal.config.ServerConfig;
import org.ostara.parser.ApplicationArguments;
import org.ostara.parser.ApplicationRunListener;
import org.ostara.parser.ConfigurableArgumentsRunListener;

public class Launcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            logger.error("Start server failed", e);
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        ApplicationRunListener listener = new ConfigurableArgumentsRunListener(args);
        ApplicationArguments arguments = listener.starting();

        if (arguments == null) {
            throw new RuntimeException("Server config file cannot be empty");
        }

        ServerConfig config = arguments.config();
        SimpleServer server = new SimpleServer(config);

        Runtime.getRuntime().addShutdownHook(new ShutdownHook<>("Simple server: " + config.getServerId(), () -> {
            server.shutdownGracefully();
            return null;
        }));

        server.start();
    }
}
