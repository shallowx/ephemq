package org.shallow.metadata.sraft;

import org.shallow.Client;
import org.shallow.ClientConfig;

public class RaftQuorumClient extends Client {

    public RaftQuorumClient(String name, ClientConfig config) {
        super(name, config);
    }

}
