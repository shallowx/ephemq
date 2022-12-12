package org.ostara.client;

import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Node;

public class ClusterTest {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterTest.class);

    @Test
    public void testQuery() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of("127.0.0.1:9127"));
        Client client = new Client("cluster-client", clientConfig);
        client.start();

        Set<Node> nodeRecords = client.getMetadataSupport()
                .queryNodeRecord(DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool().acquireWithRandomly());

        logger.info("result:{}", nodeRecords);
    }
}
