package org.shallow.internal.client;

import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class QuorumVoterClient extends Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(QuorumVoterClient.class);

    public QuorumVoterClient(String name, ClientConfig config) {
        super(name, config);
    }
}
