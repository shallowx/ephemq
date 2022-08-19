package org.shallow;

import org.shallow.internal.BrokerServer;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Server.class);

    public static void main(String[] args ) {
        try {
            run(args);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Start server failed", e);
            }
            System.exit(-1);
        }
    }

    private static void run(String[] args) throws Exception {
        ApplicationRunListener listener = new ConfigurableArgumentsRunListener(args);
        ApplicationArguments arguments = listener.starting();

        BrokerConfig config = arguments.config();
        BrokerServer server = new BrokerServer(config);

        Runtime.getRuntime().addShutdownHook(new ShutdownHook<>("broker server", () -> {
            server.shutdownGracefully();
            return  null;
        }));

        server.start();
    }
}
