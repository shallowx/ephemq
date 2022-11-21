package org.leopard;

import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.thread.ShutdownHook;
import org.leopard.internal.SimpleServer;
import org.leopard.internal.config.ServerConfig;
import org.leopard.parser.ApplicationArguments;
import org.leopard.parser.ApplicationRunListener;
import org.leopard.parser.ConfigurableArgumentsRunListener;

public class Launcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start server failed", e);
            }
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
