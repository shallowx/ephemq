package org.leopard.example.metadata;

import org.junit.jupiter.api.Test;
import org.leopard.client.Client;
import org.leopard.client.ClientConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.meta.NodeRecord;
import org.leopard.client.pool.DefaultFixedChannelPoolFactory;

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
