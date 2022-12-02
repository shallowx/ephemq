package org.leopard.example.metadata;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.leopard.client.internal.Client;
import org.leopard.client.internal.ClientConfig;
import org.leopard.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;

@SuppressWarnings("all")
public class ClusterMetadataWriterExample {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataWriterExample.class);

    @Test
    public void query() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("cluster-client", clientConfig);
        client.start();

        Set<Node> nodeRecords = client.getMetadataManager()
                .queryNodeRecord(DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool().acquireWithRandomly());

        logger.info("result:{}", nodeRecords);
    }
}
