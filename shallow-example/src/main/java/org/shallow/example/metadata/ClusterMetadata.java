package org.shallow.example.metadata;

import org.junit.jupiter.api.Test;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;

import java.util.List;
import java.util.Set;

@SuppressWarnings("all")
public class ClusterMetadata {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadata.class);

    @Test
    public void query() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("cluster-client", clientConfig);
        client.start();

        Set<NodeRecord> nodeRecords = client.getMetadataManager().queryNodeRecord(DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool().acquireWithRandomly());

        logger.info("result:{}", nodeRecords);
    }
}
